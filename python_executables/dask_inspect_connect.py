import argparse
import dask.dataframe as dd
import pandas as pd
import json
import numpy as np
import mmap

def retrieve_rows(df, method):
    if method == "GET_FIRST_ROW":
        subset = df.head(1)
    elif method == "GET_LAST_ROW":
        subset = df.tail(1)
    elif method.startswith("GET_FIRST_N_ROWS:"):
        n = int(method.split(":")[1])
        subset = df.head(n)
    elif method.startswith("GET_LAST_N_ROWS:"):
        n = int(method.split(":")[1])
        subset = df.tail(n)
    elif method.startswith("GET_ROW_RANGE:"):
        start, end = map(int, method.split(":")[1].split("_"))
        subset = df.loc[start:end+1]
    elif method == "GET_SUMMARY":
        first_five = df.head(5)
        last_five = df.tail(5)
        subset = dd.concat([first_five, last_five])
    else:
        raise ValueError("Invalid method")

    if isinstance(subset, dd.DataFrame):
        subset = subset.compute()

    # Convert all NaN values to empty strings
    subset = subset.fillna("")

    # Extract row numbers and adjust to start from 1
    row_numbers = (subset.index + 1).tolist()
    records = subset.astype(str).to_dict(orient="records")

    # Create a dictionary with row numbers as keys
    result = {str(row_number): record for row_number, record in zip(row_numbers, records)}

    return result

def retrieve_row_range_in_chunks(path, start, end):
    result = {}
    start = start - 1
    end = end - 1

    # Read specified rows using pandas directly
    df_chunk = pd.read_csv(path, skiprows=range(1, start + 1), nrows=end - start + 1, dtype=str)

    # Convert all NaN values to empty strings
    df_chunk = df_chunk.fillna("")

    # Adjust index to match original row numbers
    df_chunk.index += start

    row_numbers = (df_chunk.index + 1).tolist()
    records = df_chunk.astype(str).to_dict(orient="records")

    # Update the result dictionary
    result.update({str(row_number): record for row_number, record in zip(row_numbers, records)})

    return result

def main():
    parser = argparse.ArgumentParser(description='Retrieve specific rows from a CSV file using Dask')
    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)
    parser.add_argument('--path', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--method', type=str, required=True, help='Method to retrieve rows: GET_FIRST_ROW, GET_LAST_ROW, GET_FIRST_N_ROWS:X, GET_LAST_N_ROWS:Y, GET_ROW_RANGE:Z_A, GET_SUMMARY')
    parser.add_argument('--blocksize', type=int, default=78643200, help='Block size in bytes for chunk processing (default: 75MB)')
    parser.add_argument('--sample_size', type=int, default=10000000, help='Sample size in bytes for reading CSV (default: 10MB)')

    args = parser.parse_args()

    # Read CSV file into a Dask DataFrame
    df = dd.read_csv(args.path, dtype=str, blocksize=args.blocksize, sample=args.sample_size)

    # Retrieve rows based on the method specified
    if args.method.startswith("GET_ROW_RANGE:"):
        start, end = map(int, args.method.split(":")[1].split("_"))
        selected_rows = retrieve_row_range_in_chunks(args.path, start, end)
    else:
        selected_rows = retrieve_rows(df, args.method)

    # Convert the dictionary to a JSON string
    #selected_rows_json = json.dumps({"rows": selected_rows}, indent=4)

    #print(selected_rows_json)
    json_output = json.dumps({"rows": selected_rows}, indent=4)
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

