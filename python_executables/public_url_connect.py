import argparse
import json
import pandas as pd
from urllib.parse import urlparse, parse_qs
import mmap

def get_google_sheet_data(sheet_url):
    # Extract the base URL and sheet ID
    parsed_url = urlparse(sheet_url)
    if "spreadsheets/d/" in parsed_url.path:
        base_url = f"https://docs.google.com/spreadsheets/d/{parsed_url.path.split('/d/')[1].split('/')[0]}"
        query_params = parse_qs(parsed_url.fragment)
        gid = query_params.get("gid", ["0"])[0]  # Default to first sheet if gid is not specified
        csv_url = f"{base_url}/export?format=csv&gid={gid}"
    else:
        raise ValueError("Invalid Google Sheet URL format")

    # Read the Google Sheet data into a pandas DataFrame
    df = pd.read_csv(csv_url)

    headers = df.columns.tolist()
    rows = df.values.tolist()

    return headers, rows

def main():
    parser = argparse.ArgumentParser(description="Fetch data from a public Google Sheet.")
    parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)
    parser.add_argument('--url', type=str, required=True, help='URL of the Google Sheet')
    parser.add_argument('--url_type', type=str, choices=['GOOGLE_SHEETS'], required=True, help='Type of the URL')

    args = parser.parse_args()

    if args.url_type == 'GOOGLE_SHEETS':
        headers, rows = get_google_sheet_data(args.url)

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


    else:
        print("Unsupported URL type")

if __name__ == '__main__':
    main()

