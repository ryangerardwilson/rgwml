import argparse
import dask.dataframe as dd
import json
import mmap

def process_with_dask(uid, csv_path, groupby_column, operations, limit):
    # Read CSV file into a Dask DataFrame with all columns as objects
    df = dd.read_csv(csv_path, dtype='object', on_bad_lines='skip', low_memory=False)

    if limit:
        df = df.head(int(limit), compute=False)

    if groupby_column not in df.columns:
        print(f"Error: Column '{groupby_column}' not found in the CSV file.")
        return

    # Convert appropriate columns to numerical types for aggregation
    def safe_convert(df, columns, dtype):
        for col in columns:
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

    numerical_columns = set()
    datetime_columns = set()
    non_numerical_columns = set()

    for op, columns in operations.items():
        if 'numerical' in op or 'bool_percent' in op:
            numerical_columns.update(columns)
        elif op in ['datetime_max', 'datetime_min']:
            datetime_columns.update(columns)
        elif op in ['datetime_semi_colon_separated', 'mode', 'count_unique']:
            non_numerical_columns.update(columns)

    # Convert numerical and datetime columns only
    df = safe_convert(df, numerical_columns, 'float64')
    for col in datetime_columns:
        if col in df.columns:
            df[col] = dd.to_datetime(df[col], errors='coerce')

    # Split operations into separate DataFrames
    grouped_dfs = []

    def compute_and_rename(df, agg_dict, rename_dict):
        grouped_df = df.groupby(groupby_column).agg(agg_dict).reset_index().compute()
        grouped_df = grouped_df.rename(columns=rename_dict)
        return grouped_df

    for op, columns in operations.items():
        for col in columns:
            if col in df.columns:
                if op == 'numerical_max':
                    agg_dict = {col: 'max'}
                    rename_dict = {col: f'{col}_NUMERICAL_MAX'}
                elif op == 'numerical_min':
                    agg_dict = {col: 'min'}
                    rename_dict = {col: f'{col}_NUMERICAL_MIN'}
                elif op == 'numerical_sum':
                    agg_dict = {col: 'sum'}
                    rename_dict = {col: f'{col}_NUMERICAL_SUM'}
                elif op == 'numerical_mean':
                    agg_dict = {col: 'mean'}
                    rename_dict = {col: f'{col}_NUMERICAL_MEAN'}
                elif op == 'numerical_median':
                    agg_dict = {col: 'median'}
                    rename_dict = {col: f'{col}_NUMERICAL_MEDIAN'}
                elif op == 'numerical_std':
                    agg_dict = {col: 'std'}
                    rename_dict = {col: f'{col}_NUMERICAL_STANDARD_DEVIATION'}
                elif op == 'datetime_max':
                    agg_dict = {col: 'max'}
                    rename_dict = {col: f'{col}_DATETIME_MAX'}
                elif op == 'datetime_min':
                    agg_dict = {col: 'min'}
                    rename_dict = {col: f'{col}_DATETIME_MIN'}
                elif op == 'datetime_semi_colon_separated':
                    grouped_df = df.groupby(groupby_column)[col].apply(
                        lambda x: ';'.join(x.dropna().astype(str).replace('NaT', '')), meta=(col, 'str')
                    ).reset_index().compute()
                    grouped_df = grouped_df.rename(columns={col: f'{col}_DATETIME_SEMI_COLON_SEPARATED'})
                    grouped_dfs.append(grouped_df)
                    continue
                elif op == 'bool_percent':
                    # Ensure the column is numeric and drop non-numeric values
                    df[col] = dd.to_numeric(df[col], errors='coerce')
                    df = df.dropna(subset=[col])
                    grouped_df = df.groupby(groupby_column)[col].apply(lambda x: round((x.sum() / x.count()) * 100, 2), meta=(col, 'float64')).reset_index().compute()
                    grouped_df = grouped_df.rename(columns={col: f'{col}_BOOL_PERCENT'})
                    grouped_dfs.append(grouped_df)
                    continue
                elif op == 'mode':
                    grouped_df = df.groupby(groupby_column)[col].apply(lambda x: x.mode().iloc[0] if not x.mode().empty else None, meta=(col, 'object')).reset_index().compute()
                    grouped_df = grouped_df.rename(columns={col: f'{col}_MODE'})
                    grouped_dfs.append(grouped_df)
                    continue
                else:
                    continue

                grouped_df = compute_and_rename(df, agg_dict, rename_dict)
                grouped_dfs.append(grouped_df)

    # Handle count_unique operation separately
    count_unique_cols = operations.get('count_unique', [])
    for col in count_unique_cols:
        if col in df.columns:
            count_unique_df = df.groupby(groupby_column)[col].nunique().reset_index().compute()
            count_unique_df = count_unique_df.rename(columns={col: f'{col}_COUNT_UNIQUE'})
            grouped_dfs.append(count_unique_df)

    # Compute COUNT_TOTAL for the groupby column values
    count_total_df = df.groupby(groupby_column).size().to_frame('COUNT_TOTAL').reset_index().compute()
    grouped_dfs.append(count_total_df)

    # Compute COUNT_UNIQUE for the groupby column values
    """
    count_unique_groupby_df = df.groupby(groupby_column).apply(lambda x: x[groupby_column].nunique(), meta=(groupby_column, 'int64')).to_frame('COUNT_UNIQUE').reset_index().compute()
    grouped_dfs.append(count_unique_groupby_df)
    """
    count_unique_groupby_df = df.drop_duplicates().groupby(groupby_column).size().to_frame('COUNT_UNIQUE').reset_index().compute()
    grouped_dfs.append(count_unique_groupby_df)

    # Join all the DataFrames together
    final_grouped_df = grouped_dfs[0]
    for grouped_df in grouped_dfs[1:]:
        final_grouped_df = final_grouped_df.merge(grouped_df, on=groupby_column, how='outer')

    # Round specific columns to 2 decimal places
    for col in final_grouped_df.columns:
        if col.endswith('_NUMERICAL_SUM') or col.endswith('_NUMERICAL_MEAN') or col.endswith('_NUMERICAL_MEDIAN') or col.endswith('_NUMERICAL_STANDARD_DEVIATION'):
            final_grouped_df[col] = final_grouped_df[col].round(2)

    # Reorder columns to ensure COUNT_TOTAL and COUNT_UNIQUE follow the groupby column, and rest in alphabetical order
    cols = [groupby_column, 'COUNT_TOTAL', 'COUNT_UNIQUE'] + sorted([col for col in final_grouped_df.columns if col not in {groupby_column, 'COUNT_TOTAL', 'COUNT_UNIQUE'}])
    final_grouped_df = final_grouped_df[cols]

    # Convert all columns to string to ensure consistency
    final_grouped_df = final_grouped_df.astype(str)

    # Replace 'NaT' with empty string in all columns
    final_grouped_df.replace('NaT', '', inplace=True)

    # Prepare the final output
    headers = list(final_grouped_df.columns)
    rows = final_grouped_df.values.tolist()
    output = {
        "headers": headers,
        "rows": [[item for item in row] for row in rows],
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

def main():
    parser = argparse.ArgumentParser(description='Process a CSV file using Dask')
    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

    parser.add_argument('--path', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--group_by', type=str, required=True, help='Column name to group by')
    parser.add_argument('--numerical_max', type=str, help='Columns to find max values, separated by commas')
    parser.add_argument('--numerical_min', type=str, help='Columns to find min values, separated by commas')
    parser.add_argument('--numerical_sum', type=str, help='Columns to sum values, separated by commas')
    parser.add_argument('--numerical_mean', type=str, help='Columns to find mean values, separated by commas')
    parser.add_argument('--numerical_median', type=str, help='Columns to find median values, separated by commas')
    parser.add_argument('--numerical_std', type=str, help='Columns to find standard deviation, separated by commas')
    parser.add_argument('--datetime_max', type=str, help='Columns to find max datetime values, separated by commas')
    parser.add_argument('--datetime_min', type=str, help='Columns to find min datetime values, separated by commas')
    parser.add_argument('--datetime_semi_colon_separated', type=str, help='Columns to semi-colon separate datetime values, separated by commas')
    parser.add_argument('--mode', type=str, help='Columns to find the most frequent value, separated by commas')
    parser.add_argument('--bool_percent', type=str, help='Columns to calculate the percentage of 1s, separated by commas')
    parser.add_argument('--count_unique', type=str, help='Columns to count unique values, separated by commas')
    parser.add_argument('--limit', type=str, help='Limit the number of rows to process')

    args = parser.parse_args()

    operations = {
        'numerical_max': parse_columns(args.numerical_max) if args.numerical_max else [],
        'numerical_min': parse_columns(args.numerical_min) if args.numerical_min else [],
        'numerical_sum': parse_columns(args.numerical_sum) if args.numerical_sum else [],
        'numerical_mean': parse_columns(args.numerical_mean) if args.numerical_mean else [],
        'numerical_median': parse_columns(args.numerical_median) if args.numerical_median else [],
        'numerical_std': parse_columns(args.numerical_std) if args.numerical_std else [],
        'datetime_max': parse_columns(args.datetime_max) if args.datetime_max else [],
        'datetime_min': parse_columns(args.datetime_min) if args.datetime_min else [],
        'datetime_semi_colon_separated': parse_columns(args.datetime_semi_colon_separated) if args.datetime_semi_colon_separated else [],
        'mode': parse_columns(args.mode) if args.mode else [],
        'bool_percent': parse_columns(args.bool_percent) if args.bool_percent else [],
        'count_unique': parse_columns(args.count_unique) if args.count_unique else [],
    }

    process_with_dask(args.uid, args.path, args.group_by, operations, args.limit)

if __name__ == '__main__':
    main()

