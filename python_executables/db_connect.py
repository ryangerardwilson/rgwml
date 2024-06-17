from clickhouse_driver import Client
from google.cloud import bigquery
import sys
import json
import mmap

def create_clickhouse_connection(host, port, username, password):
    client = Client(host=host, port=port, user=username, password=password)
    return client

def query_clickhouse(client, query):
    results, columns = client.execute(query, with_column_types=True)
    return results, columns

def create_bigquery_connection(credentials_path):
    client = bigquery.Client.from_service_account_json(credentials_path)
    return client

def query_bigquery(client, query):
    query_job = client.query(query)
    results = query_job.result()
    columns = [field.name for field in results.schema]
    rows = [list(row.values()) for row in results]
    return rows, columns

def main():
    if len(sys.argv) < 3:
        print("Usage: python script.py <uid> <database_type> <database_specific_arguments>")
        sys.exit(1)

    uid = sys.argv[1]
    database_type = sys.argv[2]

    if database_type == "clickhouse":
        if len(sys.argv) != 8:
            print("Usage for ClickHouse: python script.py clickhouse <host> <port> <username> <password> <query>")
            sys.exit(1)

        host = sys.argv[3]
        port = sys.argv[4]
        username = sys.argv[5]
        password = sys.argv[6]
        query = sys.argv[7]

        client = create_clickhouse_connection(host, port, username, password)
        results, columns = query_clickhouse(client, query)

    elif database_type == "google_big_query":
        if len(sys.argv) != 5:
            print("Usage for Google BigQuery: python script.py google_big_query <credentials_path> <query>")
            sys.exit(1)

        credentials_path = sys.argv[3]
        query = sys.argv[4]

        client = create_bigquery_connection(credentials_path)
        results, columns = query_bigquery(client, query)

    else:
        print("Unsupported database type. Use 'clickhouse' or 'google_big_query'.")
        sys.exit(1)

    output = {
        "headers": columns,
        "rows": [[str(item) for item in row] for row in results]
    }

    #print(json.dumps(output))
    json_output = json.dumps(output, indent=4)
    with open('output.json', 'wb') as f:
        # Resize the file to the size of the JSON output
        f.write(b' ' * len(json_output))

    with open('output.json', 'r+b') as f:
        mm = mmap.mmap(f.fileno(), 0)
        mm.write(json_output.encode('utf-8'))
        mm.close()

if __name__ == "__main__":
    main()

