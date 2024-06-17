import argparse
import dask.dataframe as dd
import json
import mmap

def frequency_report(csv_path, columns, limit, order_by):
    # Read CSV file into a Dask DataFrame
    df = dd.read_csv(csv_path, dtype=str, blocksize=25e6)  # Adjust blocksize as necessary

    # Split columns string into a list
    columns = [col.strip() for col in columns.split(",")]

    # Generate frequency report
    frequency_report = {"unique_value_counts": {}, "column_analysis": {}}
    for col in columns:
        if col in df.columns:
            frequency = df[col].value_counts().compute()
            unique_count = frequency.size

            if limit is not None:
                top_frequency = frequency.nlargest(limit).to_dict()
            else:
                top_frequency = frequency.to_dict()

            # Sort the frequency dictionary based on the order_by flag
            if order_by == "ASC":
                sorted_frequency = dict(sorted(top_frequency.items(), key=lambda item: item[0]))
            elif order_by == "DESC":
                sorted_frequency = dict(sorted(top_frequency.items(), key=lambda item: item[0], reverse=True))
            elif order_by == "FREQ_ASC":
                sorted_frequency = dict(sorted(top_frequency.items(), key=lambda item: item[1]))
            else:  # Default to "FREQ_DESC"
                sorted_frequency = dict(sorted(top_frequency.items(), key=lambda item: item[1], reverse=True))

            # Convert frequency values to strings
            sorted_frequency = {k: str(v) for k, v in sorted_frequency.items()}

            frequency_report["unique_value_counts"][col] = str(unique_count)
            frequency_report["column_analysis"][col] = sorted_frequency

    return frequency_report

def main():
    parser = argparse.ArgumentParser(description='Generate a frequency report for specified columns in a CSV file using Dask')

    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)
    parser.add_argument('--path', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--columns', type=str, required=True, help='Comma-separated list of columns to generate frequency report for')
    parser.add_argument('--limit', type=int, default=None, help='Limit the frequency report to the top N items')
    parser.add_argument('--order_by', type=str, choices=['ASC', 'DESC', 'FREQ_ASC', 'FREQ_DESC'], default='FREQ_DESC', help='Order of the frequency report: ASC, DESC, FREQ_ASC, FREQ_DESC')

    args = parser.parse_args()
    report = frequency_report(args.path, args.columns, args.limit, args.order_by)

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

