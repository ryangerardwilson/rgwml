import argparse
import dask.dataframe as dd
import pandas as pd
import json
import mmap

def process_with_dask(uid, csv_path, groupby_column, values_from, operation, segregate_by, limit):
    # Read CSV file into a Dask DataFrame with all columns as objects
    df = dd.read_csv(csv_path, dtype='object', on_bad_lines='skip', low_memory=False)

    if limit:
        df = df.head(int(limit), compute=False)

    if groupby_column not in df.columns or values_from not in df.columns:
        print(f"Error: Column '{groupby_column}' or '{values_from}' not found in the CSV file.")
        return

    for col in segregate_by:
        if col not in df.columns:
            print(f"Error: Column '{col}' not found in the CSV file.")
            return

    # Apply segregations, treating all as categories
    for col in segregate_by:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Convert appropriate columns to numerical types for aggregation
    def safe_convert(df, col, dtype):
        if col in df.columns:
            try:
                df[col] = dd.to_numeric(df[col], errors='coerce')
                if dtype == 'int64':
                    df[col] = df[col].fillna(0).astype('int64')
                else:
                    df[col] = df[col].astype(dtype)
            except ValueError:
                print(f"Warning: Could not convert column '{col}' to {dtype}. Skipping this column.")
                df = df.drop(columns=[col])
        return df

    if operation in ['NUMERICAL_MAX', 'NUMERICAL_MIN', 'NUMERICAL_SUM', 'NUMERICAL_MEAN', 'NUMERICAL_MEDIAN', 'NUMERICAL_STANDARD_DEVIATION']:
        df = safe_convert(df, values_from, 'float64')

    if operation == 'BOOL_PERCENT':
        df = safe_convert(df, values_from, 'float64')

    if operation in ['DATETIME_MAX', 'DATETIME_MIN']:
        df[values_from] = dd.to_datetime(df[values_from], errors='coerce')

    # Define aggregation operations
    aggfunc = None
    if operation == 'COUNT':
        aggfunc = 'count'
    elif operation == 'COUNT_UNIQUE':
        aggfunc = 'nunique'
    elif operation == 'NUMERICAL_MAX':
        aggfunc = 'max'
    elif operation == 'NUMERICAL_MIN':
        aggfunc = 'min'
    elif operation == 'NUMERICAL_SUM':
        aggfunc = 'sum'
    elif operation == 'NUMERICAL_MEAN':
        aggfunc = 'mean'
    elif operation == 'NUMERICAL_MEDIAN':
        aggfunc = 'median'
    elif operation == 'NUMERICAL_STANDARD_DEVIATION':
        aggfunc = 'std'
    elif operation == 'BOOL_PERCENT':
        aggfunc = lambda x: round((x.sum() / x.count()) * 100, 2)
    else:
        print(f"Error: Unsupported operation '{operation}'.")
        return

    # Ensure columns used for segregation are properly set before pivoting
    for col in segregate_by:
        if df[col].dtype.name == 'category':
            df[col] = df[col].astype(str)

    # Create pivot table
    if segregate_by:
        pivot_df = df.compute().pivot_table(index=groupby_column, columns=list(segregate_by), values=values_from, aggfunc=aggfunc)

        # Fill NaN values with 0
        pivot_df = pivot_df.fillna(0)

        # Round values to 2 decimal places
        pivot_df = pivot_df.round(2)

        # Create column names in the desired format
        new_columns = []
        for col in pivot_df.columns.to_flat_index():
            col_name = "&&".join([f"[{key}({val})]" for key, val in zip(segregate_by, col)])
            new_columns.append(col_name)

        # Set new column names
        pivot_df.columns = new_columns

        # Add TOTAL column
        pivot_df[f'OVERALL_{operation}({values_from})'] = pivot_df.sum(axis=1).round(2)

        # Reset index and ensure groupby_column is the first column
        pivot_df = pivot_df.reset_index()

    else:
        pivot_df = df.compute().groupby(groupby_column).agg({values_from: aggfunc}).reset_index()
        pivot_df.columns = [groupby_column, f'{operation}({values_from})']

        # Fill NaN values with 0
        pivot_df = pivot_df.fillna(0)

        # Round values to 2 decimal places
        pivot_df = pivot_df.round(2)

    # Prepare the final output
    headers = list(pivot_df.columns)
    rows = pivot_df.values.tolist()
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



def parse_columns(columns_str):
    return [col.strip() for col in columns_str.split(',')]

def parse_segregate_by(segregate_by_str):
    return [col.strip() for col in segregate_by_str.split(',')]

def main():
    parser = argparse.ArgumentParser(description='Process a CSV file using Dask and create a pivot table')

    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

    parser.add_argument('--path', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--group_by', type=str, required=True, help='Column name to group by')
    parser.add_argument('--values_from', type=str, required=True, help='Column name to take values from for aggregation')
    parser.add_argument('--operation', type=str, required=True, choices=[
        'COUNT', 'COUNT_UNIQUE', 'NUMERICAL_MAX', 'NUMERICAL_MIN', 'NUMERICAL_SUM',
        'NUMERICAL_MEAN', 'NUMERICAL_MEDIAN', 'NUMERICAL_STANDARD_DEVIATION', 'BOOL_PERCENT'
    ], help='Operation to perform for aggregation')
    parser.add_argument('--segregate_by', type=str, help='Column names for segregation, separated by commas')
    parser.add_argument('--limit', type=str, help='Limit the number of rows to process')

    args = parser.parse_args()

    segregate_by = parse_segregate_by(args.segregate_by) if args.segregate_by else []

    process_with_dask(args.uid, args.path, args.group_by, args.values_from, args.operation, segregate_by, args.limit)

if __name__ == '__main__':
    main()

