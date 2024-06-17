import argparse
import dask.dataframe as dd
import json
import mmap

def calculate_unique_value_stats(df, columns):
    stats = {}
    for col in columns:
        if col not in df.columns:
            print(f"Column '{col}' not found in the CSV file.")
            continue

        # Calculate frequency of each unique value
        freq_map = df[col].value_counts().compute().to_dict()

        # Calculate statistics
        total_unique_values = len(freq_map)
        frequencies = list(freq_map.values())
        total_frequency = sum(frequencies)
        mean_frequency = total_frequency / total_unique_values if total_unique_values > 0 else 0

        # Calculate the median frequency
        sorted_frequencies = sorted(frequencies)
        mid = len(sorted_frequencies) // 2
        if len(sorted_frequencies) % 2 == 0:
            median_frequency = (sorted_frequencies[mid - 1] + sorted_frequencies[mid]) / 2.0
        else:
            median_frequency = sorted_frequencies[mid]

        # Store the statistics
        stats[col] = {
            "total_unique_values": total_unique_values,
            "mean_frequency": mean_frequency,
            "median_frequency": median_frequency
        }

    return stats

def process_csv_with_dask(csv_path, columns):
    # Read CSV file into a Dask DataFrame
    df = dd.read_csv(csv_path, dtype='object')

    # Calculate unique value statistics for specified columns
    output = calculate_unique_value_stats(df, columns)

    # Convert the statistics to JSON format and store in a single variable
    #json_output = json.dumps(stats, indent=4)
    
    # Print the JSON string
    #print(json_output)
    
    # Store the JSON string in a variable
    #json_result = json_output
    return output

def parse_columns(columns_str):
    return [col.strip() for col in columns_str.split(',')]

def main():
    parser = argparse.ArgumentParser(description='Calculate unique value statistics for specified columns in a CSV file using Dask')

    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

    parser.add_argument('--path', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--columns', type=str, required=True, help='Comma-separated list of column names to calculate statistics for')

    args = parser.parse_args()
    columns = parse_columns(args.columns)

    output = process_csv_with_dask(args.path, columns)
    # You can now use json_result as needed in your script
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

