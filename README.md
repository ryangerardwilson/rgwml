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

6. License
----------

This project is licensed under the MIT License - see the LICENSE file for details.
