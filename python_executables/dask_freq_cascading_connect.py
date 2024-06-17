import argparse
import dask.dataframe as dd
import json
import mmap

def cascading_frequency_report(csv_path, columns, limit, order_by):
    # Read CSV file into a Dask DataFrame
    df = dd.read_csv(csv_path, dtype=str, blocksize=25e6)  # Adjust blocksize as necessary

    # Split columns string into a list
    columns = [col.strip() for col in columns.split(",")]

    # Convert limit to an integer if it's not None
    if limit is not None:
        limit = int(limit)

    # Generate cascading frequency report
    def generate_cascade_report(df, columns, limit, order_by):
        if not columns:
            return None

        current_col = columns[0]
        if current_col not in df.columns:
            return None

        frequency = df[current_col].value_counts().compute()
        if limit is not None:
            frequency = frequency.nlargest(limit)
        sorted_frequency = sort_frequency(frequency, order_by)

        report = {}
        for value, count in sorted_frequency.items():
            filtered_df = df[df[current_col] == value]
            if len(columns) > 1:
                sub_report = generate_cascade_report(filtered_df, columns[1:], limit, order_by)
                if len(columns) == 2:
                    report[value] = {
                        "count": str(count),
                        f"sub_distribution({columns[1]})": sub_report
                    }
                else:
                    report[value] = {
                        "count": str(count),
                        f"sub_distribution({columns[1]})": sub_report
                    }
            else:
                report[value] = {
                    "count": str(count)
                }

        return report

    def sort_frequency(frequency, order_by):
        if order_by == "ASC":
            return dict(sorted(frequency.items(), key=lambda item: item[0]))
        elif order_by == "DESC":
            return dict(sorted(frequency.items(), key=lambda item: item[0], reverse=True))
        elif order_by == "FREQ_ASC":
            return dict(sorted(frequency.items(), key=lambda item: item[1]))
        else:  # Default to "FREQ_DESC"
            return dict(sorted(frequency.items(), key=lambda item: item[1], reverse=True))

    # Generate report
    report = generate_cascade_report(df, columns, limit, order_by)

    return report

def main():
    parser = argparse.ArgumentParser(description='Generate a cascading frequency report for specified columns in a CSV file using Dask')

    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

    parser.add_argument('--path', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--columns', type=str, required=True, help='Comma-separated list of columns to generate frequency report for')
    parser.add_argument('--limit', type=str, default=None, help='Limit the frequency report to the top N items')
    parser.add_argument('--order_by', type=str, choices=['ASC', 'DESC', 'FREQ_ASC', 'FREQ_DESC'], default='FREQ_DESC', help='Order of the frequency report: ASC, DESC, FREQ_ASC, FREQ_DESC')

    args = parser.parse_args()
    limit = int(args.limit) if args.limit is not None else None
    report = cascading_frequency_report(args.path, args.columns, limit, args.order_by)

    #print(json.dumps(report, indent=4))
    json_output = json.dumps(report, indent=4)
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

