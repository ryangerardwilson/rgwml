# RGWML (an AI, Data Science & Machine Learning Library designed to minimize developer cognitive load)

***Author: Ryan Gerard Wilson (https://ryangerardwilson.com)***

This library simplifies Data Science, Machine Learning, and Artifical Intelligence operations. It's designed to leverage the best features of RUST, in a manner that is graceful, elegant, and ticklishly fun to build upon.

1. Overview
-------------------

### `csv_utils`

- **Purpose**: Gracefully build csv files.
- **Features**: The CsvBuilder allows you to create CSV files with grace by chaining easy-to-read methods to set headers and add rows, where as the CsvResultCacher allows you to proxy cache the results of a .csv file generating function for future executions. The CsvConverter provides an elegant interface to convert various types of data strcutures, including json, to .csv.

### `df_utils`

- **Purpose**: Replicate Python Pandas library and Jypiter Notebook functionality in Rust.
- **Features**: The Query and Grouper structs ease data manipulation, transformation, filtering, sorting, and aggregation, as DataFrames. The DataFrameCacher struct allows you to gracefully cache and retrieve results of functions that return a Dataframe.

### `ai_utils`

- **Purpose**: Leverage Rust's concurrency for AI/Graph Theory based analysis.
- **Features**: Perform complex data analyses and process neural associations in parallel, harnessing Rust's performance and safety - all while playing well with the `df_utils` library.

### `api_utils`

- **Purpose**: Gracefully make and cache API calls.
- **Features**: The ApiCallBuilder struct allows you to make, cache API calls, and also manage the subsequent cached usage.

2. csv_utils
------------

The `csv_utils` module encompasses a set of utilities designed to simplify various tasks associated with CSV files. These utilities include the `CsvBuilder` for creating and managing CSV files, the `CsvConverter` for transforming JSON data into CSV format, and the `CsvResultCacher` for efficient data caching and retrieval. Each utility is tailored to enhance productivity and ease in handling CSV data in different scenarios.

- CsvBuilder: Offers a fluent interface for creating, analyzing, and saving CSV files. It simplifies interactions with CSV data, whether starting from scratch, modifying existing files, or working with DataFrame structures.

- CsvConverter: Provides a method for converting JSON data into CSV format. This utility is particularly useful for processing and saving JSON API responses as CSV files, offering a straightforward approach to data conversion. The `CsvConverter` simplifies the process of converting JSON data into a CSV format. This is particularly useful for scenarios where data is received in JSON format from an API and needs to be transformed into a more accessible and readable CSV file. To use `CsvConverter`, simply call the `from_json` method with the JSON data and the desired output file path as arguments.

- CsvResultCacher: Uses a data generator function to create or fetch data, saves it to a specified path, and keeps it for a set duration. This helps avoid unnecessary data regeneration. Imagine you have a CSV file that logs daily temperatures. You don't want to generate this file every time you access it, especially if the data doesn't change much during the day. Here's how you can use CsvResultCacher:

Easily print example synatax relating to this feature in your workflow.

    use rgwml::csv_utils::{CsvBuilder, CsvConverter, CsvResultCacher};
    use std::collections::HashMap;

    #[tokio::main]
    async fn main() {

        let _ = CsvBuilder::get_docs();
        let _ = CsvConverter::get_docs();
        let _ = CsvResultCacher::get_docs();
        std::process::exit(1);

    }

### CsvConverter

    use serde_json::json;
    use tokio;
    use rgwml::csv_utils::CsvConverter;
    use rgwml::api_utils::ApiCallBuilder;

    // Function to fetch sales data from an API
    async fn fetch_sales_data_from_api() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/sales"; // API URL to fetch sales data

        // Payload for the API call
        let payload = json!({
            "date": "2023-12-21"
        });

        // Performing the API call
        let response = ApiCallBuilder::call(method, url, None, Some(payload))
            .execute()
            .await?;

        Ok(response)
    }

    // Main function with tokio's async runtime
    #[tokio::main]
    async fn main() {
        // Fetch sales data and handle potential errors inline
        let sales_data_response = fetch_sales_data_from_api().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch sales data: {}", e);
            std::process::exit(1); // Exit the program in case of an error
        });

        // Convert the fetched JSON data to CSV
        CsvConverter::from_json(&sales_data_response, "path/to/your/file.csv")
            .expect("Failed to convert JSON to CSV"); // Handle errors in CSV conversion
    }

### CsvBuilder

#### Instantiation

Example 1: Creating a new object

    use rgwml::csv_utils::CsvBuilder;

    let builder = CsvBuilder::new()
        .set_header(&["Column1", "Column2", "Column3"])
        .add_rows(&[&["Row1-1", "Row1-2", "Row1-3"], &["Row2-1", "Row2-2", "Row2-3"]])
        .save_as("/path/to/your/file.csv");

Example 2: Load from an existing file

    use rgwml::csv_utils::CsvBuilder;

    let builder = CsvBuilder::from_csv("/path/to/existing/file.csv");

Example 3: Load from a DataFrame object

    use rgwml::csv_utils::CsvBuilder;
    use rgwml::df_utils::DataFrame;

    let data_frame = // Initialize your DataFrame here
    let builder = CsvBuilder::from_dataframe(data_frame)
        .save_as("/path/to/your/file.csv");

####  Manipulating a CsvBuilder Object for Analysis or Saving

    use rgwml::csv_utils::CsvBuilder;

    let _ = CsvBuilder::from_csv("/path/to/your/file.csv")
        .rename_columns(vec![("OLD_COLUMN", "NEW_COLUMN")])
        .drop_columns(vec!["UNUSED_COLUMN"])
        .cascade_sort(vec![("COLUMN", "ASC")])
        .where_("address","FUZZ_MIN_SCORE_70",vec!["new delhi","jerusalem"], "COMPARE_AS_TEXT") // Adjust score value to any two digit number like FUZZ_MIN_SCORE_23, FUZZ_MIN_SCORE_67, etc.
        .print_row_count()
        .save_as("/path/to/modified/file.csv");

#### Discovering Chainable Options

    let builder = CsvBuilder::new()
        .get_options(); // Outputs available options and their syntax

#### List of Flexibly Chainable Methods

    .save_as("/path/to/your/file.csv")
    .set_header(&["Column1", "Column2", "Column3"]) // Only on CsvBuilder::new() instantiations
    .add_row(&["Row1-1", "Row1-2", "Row1-3"])
    .add_rows(&[&["Row1-1", "Row1-2", "Row1-3"], &["Row2-1", "Row2-2", "Row2-3"]])
    .order_columns(vec!["Column1", "...", "Column5", "Column2"])
    .order_columns(vec!["...", "Column5", "Column2"])
    .order_columns(vec!["Column1", "Column5", "..."])
    .print_columns()
    .print_row_count()
    .print_first_row()
    .print_last_row()
    .print_rows_range(2,5)
    .print_rows()
    .print_unique("column_name")
    .cascade_sort(vec![("Column1", "DESC"), ("Column3", "ASC")])
    .drop_columns(vec!["Column1", "Column3"])
    .rename_columns(vec![("Column1", "NewColumn1"), ("Column3", "NewColumn3")])
    .where_("column1", "==", "42", "COMPARE_AS_NUMBERS")
    .where_("column1", "==", "hello", "COMPARE_AS_TEXT"),
    .where_("column1", "CONTAINS", "apples", "COMPARE_AS_TEXT")
    .where_("column1", "DOES_NOT_CONTAIN", "apples", "COMPARE_AS_TEXT")
    .where_("column1", "STARTS_WITH", "discounted", "COMPARE_AS_TEXT")
    .where_("stated_locality_address","FUZZ_MIN_SCORE_90",vec!["Shastri park","kamal nagar"], "COMPARE_AS_TEXT") // Adjust score value to any two digit number like FUZZ_MIN_SCORE_23, FUZZ_MIN_SCORE_67, etc.
    .where_("column1", ">", "23-01-01", "COMPARE_AS_TIMESTAMPS")
    .where_set("column1", "==", "hello", "COMPARE_AS_TEXT", "Column9", "greeting"), // Sets column 9's value to "greeting", where the condition is met. This syntax applies analogously to other where_ clauses as well
    .limit(10)
    .add_column_header("NewColumn1")
    .add_column_headers(vec!["NewColumn2", "NewColumn3"])

#### List of Chainable Methods that Can't Be Subsequently Chained

    .get_unique("column_name") // Returns a Vec<String>

### CsvResultCacher

    use rgwml::api_utils::ApiCallBuilder;
    use rgwml::csv_utils::{CsvBuilder, CsvResultCacher};
    use serde_json::json;
    use tokio;

    async fn generate_daily_sales_report() -> Result<(), Box<dyn std::error::Error>> {
        async fn fetch_sales_data_from_api() -> Result<String, Box<dyn std::error::Error>> {
            let method = "POST";
            let url = "http://example.com/api/sales"; // API URL to fetch sales data

            let payload = json!({
                "date": "2023-12-21"
            });

            let response = ApiCallBuilder::call(method, url, None, Some(payload))
                .execute()
                .await?;

            Ok(response)
        }

        let sales_data_response = fetch_sales_data_from_api().await?;

        // Convert the JSON response to CSV format using CsvBuilder
        let csv_builder = CsvBuilder::from_api_call(sales_data_response)
            .await
            .unwrap()
            .save_as("/path/to/daily_sales_report.csv");

        Ok(())
    }

    #[tokio::main]
    async fn main() {
        let cache_path = "/path/to/daily_sales_report.csv";
        let cache_duration_minutes = 1440; // Cache duration set to 1 day

        let result = CsvResultCacher::fetch_async(
            || Box::pin(generate_daily_sales_report()),
            cache_path,
            cache_duration_minutes,
        ).await;

        match result {
            Ok(_) => println!("Sales report is ready."),
            Err(e) => eprintln!("Failed to generate sales report: {}", e),
        }
    }

3. df_utils
-----------

### DataFrame

A `DataFrame` is a collection of data organized into a tabular structure, where each row is represented as a `HashMap`.

Each `HashMap` in the `DataFrame` corresponds to a single row in the table, with the key being the column name and the value being the data in that column for the row. The `Value` type from `serde_json` is used to allow for flexibility in the types of data that can be stored in the table, ranging from simple scalar types like strings and numbers to more complex nested structures like arrays and objects.

This structure is particularly useful for handling and manipulating structured data, especially when working with JSON data or when preparing data for serialization/deserialization.

Example


    let mut row = HashMap::new();
    row.insert("Name".to_string(), Value::String("John Doe".to_string()));
    row.insert("Age".to_string(), Value::Number(30.into()));

    let data_frame = vec![row];


#### fn data_frame_to_value_array()

Converts a DataFrame into a serde_json Value::Array.

This function takes a DataFrame as input and converts it into a Value::Array, where each element is a Value::Object constructed from the HashMap entries.

Example


    let df = vec![HashMap::from([("key1".to_string(), Value::String("value1".to_string()))])];
    let value_array = data_frame_to_value_array(df);


#### fn dataframe_to_csv()

Writes a DataFrame to a CSV file at the specified path.

This function takes a DataFrame and a file path, converts the DataFrame to CSV format, and writes it to the file.

Example


    let df = vec![HashMap::from([("key1".to_string(), Value::String("value1".to_string()))])];
    dataframe_to_csv(df, "path/to/file.csv").expect("Failed to write CSV");


### Query

`Query` struct provides a fluent interface for querying and manipulating data within a `DataFrame`. It supports operations like selecting specific columns, applying conditions to rows, limiting the
number of results, filtering rows based on their indices, and performing multi-level sorting using the `cascade_sort` method.

Fields

- `dataframe`: The DataFrame on which the queries are executed.
- `conditions`: A vector of boxed closures that define conditions for filtering rows based on column values.
- `index_conditions`: A vector of boxed closures that define conditions for filtering rows based on row indices.
- `limit`: An optional limit on the number of rows to return.
- `selected_columns`: An optional vector of columns to select in the final result.
- `order_by_sequence`: A vector of sorting criteria used for multi-level sorting through `cascade_sort`.

Example

In this example, we create a `Query` instance and utilize its various features:

- Select specific columns.
- Apply conditions on column values.
- Filter based on row indices.
- Limit the number of results.
- Apply multi-level sorting based on specified criteria using `cascade_sort`.
- Convert date-time columns to a standardized format.

Example

    use std::collections::HashMap;
    use serde_json::Value;
    use rgwml::df_utils::{Dataframe, Query};

    // Assuming DataFrame is a type that holds a collection of data.
    let df = DataFrame::new(); // Replace with actual DataFrame initialization

    let result = Query::new(df)
     .select(&["column1", "column2"]) // Selecting specific columns
     .where_("column1", "==", 42) // Adding a condition based on column value
     .where_index_range(0, 10) // Filtering rows based on their index
     .limit(5) // Limiting the results to 5 records
     .cascade_sort(vec![("column1", "DESC"), ("column2", "ASC")]) // Applying multi-level sorting
     .convert_specified_columns_to_lexicographically_comparable_timestamps(&["date_column"])
     .execute(); // Executing the query

    // `result` now contains a DataFrame with the specified columns, conditions, sorting, and limits applied.
    

    Note: This example assumes the existence of a `DataFrame` type and relevant methods. Replace placeholder code with actual implementations as per your project's context.

### Grouper

A utility for grouping rows in a DataFrame based on a specified key. `Grouper` provides a way to categorize and segment data within a DataFrame, where the DataFrame is a collection of rows, and each row is a `HashMap<String, Value>`. It simplifies the process of aggregating, analyzing, or further manipulating data based on grouped criteria.

Example

    use std::collections::HashMap;
    use rgwml::df_utils::{Grouper, DataFrame, convert_json_string_to_dataframe};

    let json_data = r#"[{"category": "Fruit", "item": "Apple"}, {"category": "Fruit", "item": "Banana"}, {"category": "Vegetable", "item": "Carrot"}]"#;
    let df = convert_json_string_to_dataframe(json_data).unwrap();

    let grouper = Grouper::new(&df);
    let grouped_dfs = grouper.group_by("category");

    // `grouped_dfs` will now contain two grouped DataFrames, one for each category (`Fruit` and `Vegetable`).


### DataFrameCacher

A utility designed for caching and retrieving data stored in a structured format known as `DataFrame`. It shines in scenarios where data generation can be time-consuming, such as fetching data from external sources or performing resource-intensive computations.

Usage

To make the most of the `DataFrameCacher`, follow these steps:

- **Create a data generator function**: Begin by creating a data generator function that returns a `Future` producing a `Result<DataFrame, Box<dyn Error>>`. This function will be responsible for generating the data you want to cache.

- **Instantiate a `DataFrameCacher` with the `fetch_async` method**: Once you have your data generator function, you can create an instance of `DataFrameCacher` by providing the data generator function, cache path, and cache duration. If the data is still valid in the cache, it will be retrieved from there. Otherwise, the data generator function will be invoked to obtain fresh data, which will then be cached for future use.

Example

Below is an example demonstrating the first step of creating a data generator function:


    use rgwml::df_utils::{DataFrame, DataFrameCacher};

    // Define your asynchronous data generation function here
    async fn generate_my_data() -> Result<DataFrame, Box<dyn std::error::Error>> {
        // Implement your data generation logic here
        Ok(vec![])
    }

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {

        let df = DataFrameCacher::fetch_async(
            || Box::pin(generate_my_data()), // Data generator function
            "/path/to/your/data.json", // Cache path
            60, // Cache duration in minutes
        ).await?;

    dbg!(df);

    Ok(())
    }

Note: The use of `|| Box` in the example is essential. It allows you to encapsulate your data generation function within a closure and a `Box`. This is required because `DataFrameCacher` expects the data generator function to have a `'static` lifetime. Closures capture their environment, so by using `|| Box`, you ensure that both the closure and the function it captures can be moved into `DataFrameCacher`, satisfying the necessary lifetime constraints.

4. ai_utils
-----------

This library provides simple AI utilities for neural association analysis. It focuses on processing and analyzing data within neural networks, with an emphasis on understanding AI decision-making processes and text analysis, optimized for a parallel computing environment.

Features

- **Direct CSV Input**: Utilize CSV file paths directly, specifying input and output column names, to facilitate neural association analysis.
- **Parallel Processing**: Leverage parallel computing to efficiently analyze neural associations, gaining insights into AI decision-making processes.

Example

    use rgwml::ai_utils::{fuzzai, SplitUpto, ShowComplications, WordLengthSensitivity};
    use std::error::Error;

    #[tokio::main]
    async fn main() {
        // Call the fuzzai function with CSV file path
        let fuzzai_result = fuzzai(
            "path/to/your/csv/file.csv",
            "input_column_name",
            "output_column_name",
            "your text to be analyzed against the training data",
            "fuzzai_analysis",
            SplitUpto::WordSetLength(2),
            ShowComplications::False,
            WordLengthSensitivity::Coefficient(0.2),
        ).await.expect("Analysis should succeed");

        dbg!(fuzzai_result);
    }

5. api_utils
------------

Easily print example synatax relating to this feature in your workflow.

    use serde_json::json;
    use rgwml::api_utils::ApiCallBuilder;
    use std::collections::HashMap;

    #[tokio::main]
    async fn main() {

        let _ = ApiCallBuilder::get_docs();
        std::process::exit(1);

    }

Examples across common API call patterns

    use serde_json::json;
    use rgwml::api_utils::ApiCallBuilder;
    use std::collections::HashMap;

    #[tokio::main]
    async fn main() {
        // Fetch and cache post request without headers
        let response = fetch_and_cache_post_request().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch data: {}", e);
            std::process::exit(1);
        });
        println!("Response: {:?}", response);

        // Fetch and cache post request with headers
        let response_with_headers = fetch_and_cache_post_request_with_headers().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch data with headers: {}", e);
            std::process::exit(1);
        });
        println!("Response with headers: {:?}", response_with_headers);

        // Fetch and cache post request with form URL encoded content type
        let response_form_urlencoded = fetch_and_cache_post_request_form_urlencoded().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch form URL encoded data: {}", e);
            std::process::exit(1);
        });
        println!("Form URL encoded response: {:?}", response_form_urlencoded);
    }

    // Example 1: Without Headers
    async fn fetch_and_cache_post_request() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let payload = json!({
            "field1": "Hello",
            "field2": 123
        });

        let response = ApiCallBuilder::call(method, url, None, Some(payload))
            .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
            .execute()
            .await?;

        Ok(response)
    }

    // Example 2: With Headers
    async fn fetch_and_cache_post_request_with_headers() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let headers = json!({
            "Content-Type": "application/json",
            "Authorization": "Bearer your_token_here"
        });
        let payload = json!({
            "field1": "Hello",
            "field2": 123
        });

        let response = ApiCallBuilder::call(method, url, Some(headers), Some(payload))
            .maintain_cache(30, "/path/to/post_with_headers_cache.json") // Uses cache for 30 minutes
            .execute()
            .await?;

        Ok(response)
    }

    // Example 3: With application/x-www-form-urlencoded Content-Type
    async fn fetch_and_cache_post_request_form_urlencoded() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let headers = json!({
            "Content-Type": "application/x-www-form-urlencoded"
        });
        let payload = HashMap::from([
            ("field1", "value1"),
            ("field2", "value2"),
        ]);

        let response = ApiCallBuilder::call(method, url, Some(headers), Some(payload))
            .maintain_cache(30, "/path/to/post_form_urlencoded_cache.json") // Uses cache for 30 minutes
            .execute()
            .await?;

        Ok(response)
    }



6. License
----------

This project is licensed under the MIT License - see the LICENSE file for details.
