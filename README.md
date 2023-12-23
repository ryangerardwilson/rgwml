# RGWML (an AI, Data Science & Machine Learning Library designed to minimize developer cognitive load)

***Author: Ryan Gerard Wilson (https://ryangerardwilson.com)***

This library simplifies Data Science, Machine Learning, and Artifical Intelligence operations. It's designed to leverage the best features of RUST, in a manner that is graceful, elegant, and ticklishly fun to build upon.

1. Overview
-----------

## `csv_utils`

- **Purpose**: A Comprehensive Toolkit for CSV File Management, in AI/ML pipelines.
- **Features**: `csv_utils` offers a powerful suite of tools designed for efficient and flexible handling of CSV files. Key components include:
  - **CsvBuilder**: A versatile builder for creating and manipulating CSV files, facilitating:
    - **Easy Initialization**: Start with a new CSV or load from an existing file.
    - **Custom Headers and Rows**: Set custom headers and add rows effortlessly.
    - **Advanced Data Manipulation**: Rename, drop, and reorder columns, sort data, and apply complex filters like fuzzy matching and timestamp comparisons.
    - **Chainable Methods**: Combine multiple operations in a fluent and readable manner.
    - **Data Analysis Aids**: Count rows, print specific rows, ranges, or unique values for quick analysis.
    - **Flexible Saving Options**: Save your modified CSV to a desired path.
  - **CsvResultCacher**: Cache results of CSV operations, enhancing performance for repetitive tasks.
  - **CsvConverter**: Seamlessly convert various data formats like JSON into CSV, expanding the utility of your data.

## `ai_utils`

- **Purpose**: Leverage Rust's concurrency for AI/Graph Theory-based analysis.
- **Features**: 
  - Conduct complex data analyses and process neural networks in parallel.
  - Utilize Rust's performance and safety features.
  - Works directly with CSV files, with an elegant syntax, for model training.

## `api_utils`

- **Purpose**: Gracefully make and cache API calls.
- **Features**: 
  - **ApiCallBuilder**: Make and cache API calls effortlessly, and manage cached data for efficient API usage.

## `loop_utils`

- **Purpose**: Simplify asynchronous operations in loops.
- **Features**: 
  - **FutureLoop**: Handle multiple tasks simultaneously when working with lists or collections, while working with a fluent interface.

2. csv_utils
------------

The `csv_utils` module encompasses a set of utilities designed to simplify various tasks associated with CSV files. These utilities include the `CsvBuilder` for creating and managing CSV files, the `CsvConverter` for transforming JSON data into CSV format, and the `CsvResultCacher` for efficient data caching and retrieval. Each utility is tailored to enhance productivity and ease in handling CSV data in different scenarios.

- CsvBuilder: Offers a fluent interface for creating, analyzing, and saving CSV files. It simplifies interactions with CSV data, whether starting from scratch, modifying existing files, etc.

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

####  Manipulating a CsvBuilder Object for Analysis or Saving

    use rgwml::csv_utils::CsvBuilder;

    let _ = CsvBuilder::from_csv("/path/to/your/file.csv")
        .rename_columns(vec![("OLD_COLUMN", "NEW_COLUMN")])
        .drop_columns(vec!["UNUSED_COLUMN"])
        .cascade_sort(vec![("COLUMN", "ASC")])
        .where_("address","FUZZ_MIN_SCORE_70",vec!["new delhi","jerusalem"], "COMPARE_AS_TEXT") // Adjust score value to any two digit number like FUZZ_MIN_SCORE_23, FUZZ_MIN_SCORE_67, etc.
        .print_row_count()
        .save_as("/path/to/modified/file.csv");


#### Chainable Options

    CsvBuilder::from_csv("/path/to/your/file1.csv")
    .set_header(vec!["Header1", "Header2", "Header3"])
    .add_column_header("NewColumn1")
    .add_column_headers(vec!["NewColumn2", "NewColumn3"])
    .order_columns(vec!["Column1", "...", "Column5", "Column2"])
    .order_columns(vec!["...", "Column5", "Column2"])
    .order_columns(vec!["Column1", "Column5", "..."])
    .drop_columns(vec!["Column1", "Column3"])
    .rename_columns(vec![("Column1", "NewColumn1"), ("Column3", "NewColumn3")])
    .add_row(vec!["Row1-1", "Row1-2", "Row1-3"])
    .add_rows(vec![vec!["Row1-1", "Row1-2", "Row1-3"], vec!["Row2-1", "Row2-2", "Row2-3"]])
    .limit(10)
    .cascade_sort(vec![("Column1", "DESC"), ("Column3", "ASC")])
    .where_("column1", "==", "42", "COMPARE_AS_NUMBERS")
    .where_("column1", "==", "hello", "COMPARE_AS_TEXT"),
    .where_("column1", "CONTAINS", "apples", "COMPARE_AS_TEXT")
    .where_("column1", "DOES_NOT_CONTAIN", "apples", "COMPARE_AS_TEXT")
    .where_("column1", "STARTS_WITH", "discounted", "COMPARE_AS_TEXT")
    .where_("stated_locality_address","FUZZ_MIN_SCORE_90",vec!["Shastri park","kamal nagar"], "COMPARE_AS_TEXT") // Adjust score value to any two digit number like FUZZ_MIN_SCORE_23, FUZZ_MIN_SCORE_67, etc.
    .where_("column1", ">", "23-01-01", "COMPARE_AS_TIMESTAMPS")
    .where_set("column1", "==", "hello", "COMPARE_AS_TEXT", "Column9", "greeting"), // Sets column 9's value to "greeting", where the condition is met. This syntax applies analogously to other where_ clauses as well
    .print_columns()
    .print_row_count()
    .print_first_row()
    .print_last_row()
    .print_rows_range(2,5)
    .print_rows()
    .print_unique("column_name")
    .save_as("/path/to/your/file2.csv")

#### Extract a Vector `Vec<String>` List

These methods return a list, and hence, can not be subsequently chained.

    CsvBuilder::from_csv("/path/to/your/file1.csv")
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

3. ai_utils
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
            "path/to/your/model/training/csv/file.csv",
            "model_training_input_column_name",
            "model_training_output_column_name",
            "your text to be analyzed against the training data model",
            "your task description: clustering customer complaints",
            SplitUpto::WordSetLength(2), // Set the minimum word length of combination value to split the training input data during the analysis
            ShowComplications::False, // Set to True to see inner workings of the model
            WordLengthSensitivity::Coefficient(0.2), // Set to Coefficient::None to disregard differences in the word length of the training input and the text being analyzed; Increase the coefficient to give higher weightage to matches with similar word length
        ).await.expect("Analysis should succeed");

        dbg!(fuzzai_result);
    }

4. api_utils
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

5. loop_utils
-------------

### FutureLoop

`FutureLoop` provides a fluent interface to do multiple things at once (asynchronously) when dealing with a list or collection of items. 

    use rgwml::loop_utils::FutureLoop;

    #[tokio::main]
    async fn main() {
        let data = vec![1, 2, 3, 4, 5];

        // The future_for loop lets you go through each item in a list and do something with it, all at the same time.
    
        let results = FutureLoop::future_for(data, |num| async move {
        num * 2 // Double each number
        }).await;
        println!("Results: {:?}", results);
    }

6. License
----------

This project is licensed under the MIT License - see the LICENSE file for details.
