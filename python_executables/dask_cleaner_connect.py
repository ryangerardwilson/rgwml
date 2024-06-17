import argparse
import dask.dataframe as dd
import pandas as pd
import json
from collections import defaultdict
from datetime import datetime
import heapq
import mmap

def validate_value(value, rule):
    if pd.isna(value):
        return False
    
    if rule == "IS_NUMERICAL_VALUE":
        return value.replace('.', '', 1).isdigit()
    elif rule == "IS_POSITIVE_NUMERICAL_VALUE":
        return value.replace('.', '', 1).isdigit() and float(value) > 0
    elif rule.startswith("IS_LENGTH:"):
        length = int(rule.split(":")[1])
        return len(value) == length
    elif rule.startswith("IS_MIN_LENGTH:"):
        min_length = int(rule.split(":")[1])
        return len(value) >= min_length
    elif rule.startswith("IS_MAX_LENGTH:"):
        max_length = int(rule.split(":")[1])
        return len(value) <= max_length
    elif rule == "IS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER":
        return len(value) == 10 and value.isdigit() and value[0] in "6789" and len(set(value)) >= 3
    elif rule == "IS_NOT_AN_EMPTY_STRING":
        return bool(value)
    elif rule == "IS_DATETIME_PARSEABLE":
        formats = [
            "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d",
            "%d-%m-%Y", "%Y-%m-%d %H:%M:%S.%f", "%b %d, %Y"
        ]
        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False
    else:
        return False

def validate_row(row, column_rules, non_compliant_counts):
    is_compliant = True
    for col, rule_list in column_rules.items():
        for rule in rule_list:
            if not validate_value(row[col], rule):
                non_compliant_counts[col][row[col]] += 1
                is_compliant = False
                break  # Stop checking other rules for this column
    return is_compliant

def process_csv_with_dask(csv_path, rules, action, show_unclean_examples_in_report):
    # Read CSV file into a Dask DataFrame
    df = dd.read_csv(csv_path, dtype=str, blocksize=25e6)  # Adjust blocksize as necessary
    #print(df)

    """
    # Count the number of NaN/None values in the 'mobile' column
    nan_count = df['mobile'].isna().sum().compute()
    # Print the result
    print(f"Number of NaN/None values in the 'mobile' column: {nan_count}")

    # Count the number of values in the 'mobile' column that have 10 digits and start with either 6, 7, 8, or 9
    valid_mobile_count = df['mobile'].str.match(r'^[6789]\d{9}$').sum().compute()
    print(f"Number of valid 10-digit mobile numbers starting with 6, 7, 8, or 9: {valid_mobile_count}")

    # Count the number of values in the 'days_since_first_payment' column that have a length of 2 digits
    two_digit_days_count = df['days_since_first_payment'].astype(str).str.len().eq(2).sum().compute()
    print(f"Number of values in the 'days_since_first_payment' column with a length of 2 digits: {two_digit_days_count}")
    """

    column_rules = {col: rule_list for col, rule_list in rules}

    non_compliant_counts = defaultdict(lambda: defaultdict(int))

    # Validate each row and add a compliance column
    df['is_compliant'] = df.map_partitions(lambda df_part: df_part.apply(lambda row: validate_row(row, column_rules, non_compliant_counts), axis=1))

    # Compute the results
    results = df.compute()
    initial_total_rows = len(results)

    # Filter out non-compliant rows
    results = results[results['is_compliant']].drop(columns=['is_compliant']).reset_index(drop=True)
    #print(results)
    total_rows_after_cleaning = len(results)

    report = None
    if action != 'CLEAN':
        # Generate the cleanliness report
        report = {
            "total_rows": str(initial_total_rows),
            "total_rows_after_cleaning": str(total_rows_after_cleaning),
            "percentage_row_reduction_after_cleaning": f"{round((initial_total_rows - total_rows_after_cleaning) / initial_total_rows * 100, 2):.2f}",
            "column_cleanliness_analysis": {}
        }
        
        column_analysis = {}
        NAN_CONDITION_TRIGGERED = False  # Initialize the flag

        for col, values in non_compliant_counts.items():
            rules = column_rules[col]
            total_unique_values = len(values) - 2
            total_non_compliant = sum(values.values()) - 2
            percentage_non_compliant = (total_non_compliant / initial_total_rows) * 100

            analysis = {
                "rules": rules,
                "total_unique_values": str(total_unique_values),
                "total_non_compliant": str(total_non_compliant),
                "percentage_non_compliant": f"{percentage_non_compliant:.2f}",
            }

            if show_unclean_examples_in_report:
                top_unclean_values = heapq.nlargest(10, values.items(), key=lambda x: x[1])
                top_unclean_values_filtered = {str(value): count for value, count in top_unclean_values if pd.notna(value) and value != 'a'}
                analysis["top_10_max_freq_unclean_values"] = top_unclean_values_filtered
                if total_non_compliant > 0 and top_unclean_values_filtered == {}:
                    analysis["top_10_max_freq_unclean_values"] = { "": total_non_compliant }

            column_analysis[col] = analysis
            report["column_cleanliness_analysis"][col] = analysis

        report["column_cleanliness_analysis"] = column_analysis

        # Add the report to your output or log it as needed
        #print(report)


    if action == 'ANALYZE':
        json_report = json.dumps({"report": report}, indent=4)
        #print(json_report)
        return {"report": report}
    elif action == 'CLEAN':
        headers = list(results.columns)
        rows = results.fillna('').values.tolist()
        clean_data = {
            "headers": headers,
            "rows": [[str(item) for item in row] for row in rows]
        }
        json_data = json.dumps(clean_data, indent=4)
        #print(json_data)
        return clean_data
    elif action == 'ANALYZE_AND_CLEAN':
        headers = list(results.columns)
        rows = results.fillna('').values.tolist()
        clean_data_with_report = {
            "headers": headers,
            "rows": [[str(item) for item in row] for row in rows],
            "report": report
        }
        json_data_with_report = json.dumps(clean_data_with_report, indent=4)
        #print(report)
        return clean_data_with_report

def parse_rules(rules_str):
    rules = []
    for rule_pair in rules_str.split(';'):
        column, *rule_list = rule_pair.split(':')
        rule_list = ':'.join(rule_list).split(',')
        rules.append((column.strip(), rule_list))
    return rules

def main():
    parser = argparse.ArgumentParser(description='Validate CSV columns based on specified rules and generate a cleanliness report using Dask')

    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

    parser.add_argument('--path', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--rules', type=str, required=True, help='Validation rules in the format "Column1:Rule1,Rule2;Column2:Rule3,Rule4"')
    parser.add_argument('--action', type=str, required=True, choices=['CLEAN', 'ANALYZE', 'ANALYZE_AND_CLEAN'], help='Action to perform: CLEAN, ANALYZE, or ANALYZE_AND_CLEAN')
    parser.add_argument('--show_unclean_values_in_report', type=str, choices=['TRUE', 'FALSE'], default='FALSE', help='Flag to show unclean values in the report')

    args = parser.parse_args()
    rules = parse_rules(args.rules)
    show_unclean_values_in_report = args.show_unclean_values_in_report == 'TRUE'

    json_output = process_csv_with_dask(args.path, rules, args.action, show_unclean_values_in_report)
    # You can now use json_result as needed in your script
    #print(json.dumps(json_result, indent=4))

    json_output = json.dumps(json_output, indent=4)
    """
    with open('output.json', 'wb') as f:
        # Resize the file to the size of the JSON output
        f.write(b' ' * len(json_output))

    with open('output.json', 'r+b') as f:
        mm = mmap.mmap(f.fileno(), 0)
        mm.write(json_output.encode('utf-8'))
        mm.close()
    """
    filename = f"rgwml_{args.uid}.json"
    with open(filename, 'wb') as f:
        # Resize the file to the size of the JSON output
        f.write(b' ' * len(json_output))

    with open(filename, 'r+b') as f:
        mm = mmap.mmap(f.fileno(), 0)
        mm.write(json_output.encode('utf-8'))
        mm.close()



if __name__ == '__main__':
    main()

