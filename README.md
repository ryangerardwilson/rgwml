# RGWML (an AI, Data Science & Machine Learning Library designed to minimize developer cognitive load)

***Author: Ryan Gerard Wilson (https://ryangerardwilson.com)***

This library simplifies Data Science, Machine Learning, and Artifical Intelligence operations. It's designed to leverage the best features of RUST, in a manner that is graceful, elegant, and ticklishly fun to build upon.

1. Overview
-------------------

### `df_utils`
- **Purpose**: Replicate Python Pandas library and Jypiter Notebook functionality in Rust.
- **Features**: The Query and Grouper structs ease data manipulation, transformation, filtering, sorting, and aggregation, as DataFrames. The DataFrameCacher struct allows you to gracefully cache and retrieve results of functions that return a Dataframe.

### `ai_utils`
- **Purpose**: Leverage Rust's concurrency for AI/Graph Theory based analysis.
- **Features**: Perform complex data analyses and process neural associations in parallel, harnessing Rust's performance and safety - all while playing well with the `df_utils` library.

### `api_utils`
- **Purpose**: Gracefully make and cache API calls.
- **Features**: The ApiCallBuilder struct allows you to make, cache API calls, and also manage the subsequent cached usage.

### `csv_utils`
- **Purpose**: Gracefully build csv files.
- **Features**: The CsvBuilder struct allows you to create CSV files with grace by chaining easy-to-read methods to set headers and add rows.

2. df_utils
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

1. **Create a data generator function**: Begin by creating a data generator function that returns a `Future` producing a `Result<DataFrame, Box<dyn Error>>`. This function will be responsible for generating the data you want to cache.

2. **Instantiate a `DataFrameCacher` with the `fetch_async` method**: Once you have your data generator function, you can create an instance of `DataFrameCacher` by providing the data generator function, cache path, and cache duration. If the data is still valid in the cache, it will be retrieved from there. Otherwise, the data generator function will be invoked to obtain fresh data, which will then be cached for future use.

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

3. ai_utils
-----------

Provides simple AI utilities for neural association analysis. It offers tools to process and analyze data in the context of neural networks, with a focus on understanding decision-making processes and text analysis in a parallel computing environment.

Features

- **Convert DataFrames**: Transform your data into a format suitable for neural association analysis.
- **Parallel Processing**: Analyze neural associations in parallel, revealing insights into AI decision-making.

Usage

First, convert your data into a suitable `DataFrame` format. Then, analyze the data using `fuzzai` for concurrent neural association analysis.

Example


    use rgwml::ai_utils::{NeuralAssociations2DDataFrameConfig, create_neural_associations_2d_df, fuzzai, SplitUpto, ShowComplications, WordLengthSensitivity};
    use rgwml::df_utils::DataFrame;
    use std::collections::HashMap;
    use serde_json::Value;

    #[tokio::main]
    async fn main() {
        // Prepare the raw data
        let mut data_frame = Vec::new();
        let mut record = HashMap::new();
        record.insert("address".to_string(), Value::String("123 Main St".to_string()));
        record.insert("name".to_string(), Value::String("John Doe".to_string()));
        data_frame.push(record);

        // Configure and convert the DataFrame
        let config = NeuralAssociations2DDataFrameConfig {
            input_column: "address",
            output_column: "name",
        };
    
        let neural_association_df = create_neural_associations_2d_df(data_frame, config);

        // Analyze using fuzzai
        let text_in_focus = "123 Main St";
        let task_name = "Address Analysis";
        let result = fuzzai(
            neural_association_df,
            text_in_focus,
            task_name,
            SplitUpto::WordSetLength(2),
            ShowComplications::False,
         WordLengthSensitivity::Coefficient(0.2)
     ).await.expect("Analysis should succeed");

     dbg!(result);
}


This integrated example demonstrates the full process of data transformation and analysis, highlighting the capabilities of the `rgwml` library in neural association studies. This library is perfect for applications where AI's interpretation of data patterns and decision-making processes are crucial.

4. api_utils
------------

This module features the APICallBuilder a fluent interface to build API requests with support for method chaining. It simplifies the process by allowing you to specify both headers and payload as `serde_json::Value`. This approach is convenient when dealing with JSON data, making it easy to construct requests dynamically. If caching is enabled, responses are stored and reused for subsequent requests made within the specified cache duration.

Example 1: Without Headers

    use serde_json::json;
    use rgwml::api_utils::ApiCallBuilder;

    #[tokio::main]
    async fn main() {
        let method = "POST"; // Or "GET"
        let url = "http://example.com/api/submit";
        let payload = json!({
            "field1": "Hello",
            "field2": 123
        });
        let response = ApiCallBuilder::call(
            method,
            url,
            None, // No custom headers
            Some(payload)
        ).maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
        .execute()
        .await
        .unwrap();
     
        dbg!(response);
    }


Example 2: With Headers

    use reqwest::Method;
    use serde_json::json;
    use rgwml::api_utils::ApiCallBuilder;

    #[tokio::main]
    async fn main() {
        let method = "POST"; // Or "GET"
        let url = "http://example.com/api/submit";
        let headers = json!({
            "Content-Type": "application/json",
            "Authorization": "Bearer your_token_here"
        });
        let payload = json!({
            "field1": "Hello",
            "field2": 123
        });
        let response = ApiCallBuilder::call(
             method,
             url,
             Some(headers), // Custom headers
             Some(payload)
         )
         .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
         .execute()
         .await
         .unwrap();

        dbg!(response);
    }

Example 3: With application/x-www-form-urlencoded Content-Type

    use serde_json::json;
    use rgwml::api_utils::ApiCallBuilder;
    use std::collections::HashMap;

    #[tokio::main]
    async fn main() {
        let method = "POST"; // Or "GET"
        let url = "http://example.com/api/submit";
        let headers = json!({
            "Content-Type": "application/x-www-form-urlencoded"
        });
        let payload = json!({
            "field1": "value1",
            "field2": "value2"
        });
        let response = ApiCallBuilder::call(
            method,            
            url, 
            Some(headers),
            Some(payload)
        ).maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
         .execute()
         .await
         .unwrap();
        
        dbg!(response);
    }

These examples demonstrate how to use the ApiCallBuilder with and without custom headers. Since the headers and payload are specified as `serde_json::Value`, it offers flexibility in constructing various types of requests.

Note: Be cautious when caching POST requests, as they typically send unique data each time. Caching is most effective when the same request is likely to yield the same response.

5. csv_utils
------------

The `CsvBuilder` in the `rgwml::csv_utils` module offers a fluent interface for creating, analyzing, and saving CSV files. It simplifies interactions with CSV data, whether starting from scratch, modifying existing files, or working with DataFrame structures.

### Instantiating a CsvBuilder Object

Example 1: Creating a new object

    use rgwml::csv_utils::CsvBuilder;

    let builder = CsvBuilder::new()
        .set_header(&["Column1", "Column2", "Column3"])
        .add_rows(&[&["Row1-1", "Row1-2", "Row1-3"], &["Row2-1", "Row2-2", "Row2-3"]])
        .save_as("/path/to/your/file.csv");

Example 2: Loadfrom an existing file

    use rgwml::csv_utils::CsvBuilder;

    let builder = CsvBuilder::from_csv("/path/to/existing/file.csv");

Example 3: Load from a DataFrame object

    use rgwml::csv_utils::CsvBuilder;
    use rgwml::df_utils::DataFrame;

    let data_frame = // Initialize your DataFrame here
    let builder = CsvBuilder::from_dataframe(data_frame)
        .set_header(&["Column1", "Column2", "Column3"])
        .save_as("/path/to/your/file.csv"); 


### Manipulating a CsvBuilder Object for Analysis or Saving

    use rgwml::csv_utils::CsvBuilder;

    let _ = CsvBuilder::from_csv("/path/to/your/file.csv")
        .rename_columns(vec![("OLD_COLUMN", "NEW_COLUMN")])
        .drop_columns(vec!["UNUSED_COLUMN"])
        .cascade_sort(vec![("COLUMN", "ASC")])
        .where_("address","FUZZ_MIN_SCORE_70",vec!["new delhi","jerusalem"], "COMPARE_AS_TEXT") // Adjust score value to any two digit number like FUZZ_MIN_SCORE_23, FUZZ_MIN_SCORE_67, etc.
        .print_row_count()
        .save_as("/path/to/modified/file.csv");

### Discovering Chainable Options

    let builder = CsvBuilder::new()
        .get_options(); // Outputs available options and their syntax

#### Chainable Options in `CsvBuilder`

- **`.save_as(path: &str)`**: Saves the current state of the CSV to a specified file path.

- **`.set_header(columns: &[&str])`**: Sets the header (column names) of the CSV file. Typically used with new CSV files.

- **`.add_row(row: &[&str])`**: Adds a single row to the CSV file.

- **`.add_rows(rows: &[&[&str]])`**: Adds multiple rows to the CSV file.

- **`.order_columns(order: Vec<&str>)`**: Orders columns in the specified sequence. The '...' syntax can be used to keep remaining columns in their original order.

- **`.print_columns()`**: Prints the names of the columns in the CSV file.

- **`.print_row_count()`**: Prints the total number of rows in the CSV file.

- **`.print_first_row()`**: Prints the first row of the CSV file in a JSON-like format.

- **`.print_last_row()`**: Prints the last row of the CSV file in a JSON-like format.

- **`.print_rows_range(start: usize, end: usize)`**: Prints a range of rows, specified by start and end indices.

- **`.print_rows()`**: Prints all rows in the CSV file in a JSON-like format.

- **`.cascade_sort(sort_order: Vec<(&str, &str)>)`**: Sorts the data in the CSV file based on specified columns and sort orders (ASC/DESC).

- **`.drop_columns(columns: Vec<&str>)`**: Removes specified columns from the CSV file.

- **`.rename_columns(rename_map: Vec<(&str, &str)>)`**: Renames columns as specified in the provided mapping.

- **`.where_(column: &str, operator: &str, value: T, comparison_type: &str)`**: Filters rows based on a condition, supporting text, numeric, and timestamp comparisons. The value parameter accepts any type T that implements the CompareValue trait, allowing for flexible comparisons.

6. License
----------

This project is licensed under the MIT License - see the LICENSE file for details.



