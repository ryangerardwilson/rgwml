import argparse
import json
import h5py
import pandas as pd
import mmap

def read_h5_data(file_path, dataset_identifier, identifier_type):
    with h5py.File(file_path, 'r') as f:
        if identifier_type == "DATASET_NAME":
            if dataset_identifier in f:
                dataset = f[dataset_identifier][:]
            else:
                raise ValueError(f"Dataset {dataset_identifier} not found in the file.")
        elif identifier_type == "DATASET_ID":
            dataset_names = list(f.keys())
            try:
                index = int(dataset_identifier)
                if 0 <= index < len(dataset_names):
                    dataset_name = dataset_names[index]
                    dataset = f[dataset_name][:]
                else:
                    raise ValueError(f"Dataset index {index} out of range.")
            except ValueError:
                raise ValueError("Invalid dataset ID.")
        else:
            raise ValueError("Invalid identifier type.")

        headers = [f"Column {i}" for i in range(dataset.shape[1])] if dataset.ndim > 1 else ["Value"]
        rows = dataset.tolist()

    return headers, rows

def read_pandas_h5(file_path, dataset_identifier, identifier_type):
    with h5py.File(file_path, 'r') as f:
        dataset_names = list(f.keys())
        if identifier_type == "DATASET_ID":
            try:
                index = int(dataset_identifier)
                if 0 <= index < len(dataset_names):
                    key = dataset_names[index]
                else:
                    raise ValueError(f"Dataset index {index} out of range.")
            except ValueError:
                raise ValueError("Invalid dataset ID.")
        elif identifier_type == "DATASET_NAME":
            key = dataset_identifier
            if key not in dataset_names:
                raise ValueError(f"Dataset {key} not found in the file.")
        else:
            raise ValueError("Invalid identifier type.")

    df = pd.read_hdf(file_path, key=key)
    headers = df.columns.tolist()
    rows = df.values.tolist()
    return headers, rows

def main():
    parser = argparse.ArgumentParser(description="Fetch data from an HDF5 file.")
    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

    parser.add_argument('--path', type=str, required=True, help='Path to the file')
    parser.add_argument('--dc_type', type=str, choices=['H5'], required=True, help='Type of the data container')
    parser.add_argument('--h5_dataset_identifier', type=str, required=True, help='Name or index of the dataset')
    parser.add_argument('--h5_identifier_type', type=str, choices=['DATASET_NAME', 'DATASET_ID'], required=True, help='Type of the dataset identifier')

    args = parser.parse_args()

    try:
        headers, rows = read_h5_data(args.path, args.h5_dataset_identifier, args.h5_identifier_type)
    except Exception as e:
        #print(f"Failed to read as normal H5: {e}")
        #print("Attempting to read as PANDAS H5...")
        headers, rows = read_pandas_h5(args.path, args.h5_dataset_identifier, args.h5_identifier_type)

    # Format output
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

