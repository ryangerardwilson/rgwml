import argparse
import dask.dataframe as dd
import json
import mmap

def perform_difference(uid, file_a_path, file_b_path, difference_type, file_a_ref_column, file_b_ref_column):
    # Read CSV files into Dask DataFrames
    df_a = dd.read_csv(file_a_path, dtype='object', on_bad_lines='skip', low_memory=False)
    df_b = dd.read_csv(file_b_path, dtype='object', on_bad_lines='skip', low_memory=False)

    # Ensure the reference columns are present in both DataFrames
    if file_a_ref_column not in df_a.columns or file_b_ref_column not in df_b.columns:
        print(f"Error: Reference column '{file_a_ref_column}' or '{file_b_ref_column}' not found in the CSV files.")
        return

    # Perform the difference operation
    if difference_type == 'NORMAL':
        # Rows in df_a but not in df_b
        result = df_a[~df_a[file_a_ref_column].isin(df_b[file_b_ref_column])]
    elif difference_type == 'SYMMETRIC':
        # Rows in df_a but not in df_b and rows in df_b but not in df_a
        result_a_not_b = df_a[~df_a[file_a_ref_column].isin(df_b[file_b_ref_column])]
        result_b_not_a = df_b[~df_b[file_b_ref_column].isin(df_a[file_a_ref_column])]
        result = dd.concat([result_a_not_b, result_b_not_a])
    else:
        print(f"Error: Unsupported difference type '{difference_type}'.")
        return

    # Compute the result
    result = result.compute()

    # Replace NaN values with empty strings
    result = result.fillna('')

    # Prepare the final output
    headers = list(result.columns)
    rows = result.values.tolist()
    output = {
        "headers": headers,
        "rows": [[str(item) for item in row] for row in rows],
    }

    #print(json.dumps(output, indent=4))
    json_output = json.dumps(output, indent=4)
    """
    with open('output.json', 'wb') as f:
        # Resize the file to the size of the JSON output
        f.write(b' ' * len(json_output))

    with open('output.json', 'r+b') as f:
        mm = mmap.mmap(f.fileno(), 0)
        mm.write(json_output.encode('utf-8'))
        mm.close()
    """
    filename = f"rgwml_{uid}.json"
    with open(filename, 'wb') as f:
        # Resize the file to the size of the JSON output
        f.write(b' ' * len(json_output))

    with open(filename, 'r+b') as f:
        mm = mmap.mmap(f.fileno(), 0)
        mm.write(json_output.encode('utf-8'))
        mm.close()



def main():
    parser = argparse.ArgumentParser(description='Perform MySQL-like differences on CSV file datasets using Dask')

    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

    parser.add_argument('--file_a_path', type=str, required=True, help='Path to the first CSV file')
    parser.add_argument('--file_b_path', type=str, required=True, help='Path to the second CSV file')
    parser.add_argument('--difference_type', type=str, required=True, choices=['NORMAL', 'SYMMETRIC'], help='Type of difference operation to perform')
    parser.add_argument('--file_a_ref_column', type=str, required=True, help='Reference column in the first CSV file')
    parser.add_argument('--file_b_ref_column', type=str, required=True, help='Reference column in the second CSV file')

    args = parser.parse_args()

    perform_difference(args.uid, args.file_a_path, args.file_b_path, args.difference_type, args.file_a_ref_column, args.file_b_ref_column)

if __name__ == '__main__':
    main()

