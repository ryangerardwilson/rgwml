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

- CsvResultCacher: Uses a data generator function to create or fetch data, saves it to a specified path, and keeps it for a set duration. This helps avoid unnecessary data regeneration. Imagine you have a CSV file that logs daily temperatures. You don't want to generate this file every time you access it, especially if the data doesn't change much during the day.

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

Example 3: Load from an xls file

    use rgwml::csv_utils::CsvBuilder;
        
    let builder = CsvBuilder::from_xls("/path/to/existing/file.xls", 1); // Loads from the first sheet of the .xls file

####  Manipulating a CsvBuilder Object for Analysis or Saving

    use rgwml::csv_utils::{Exp, ExpVal, CsvBuilder, CsvConverter, CsvResultCacher};

    let _ = CsvBuilder::from_csv("/path/to/your/file.csv")
        .rename_columns(vec![("OLD_COLUMN", "NEW_COLUMN")])
        .drop_columns(vec!["UNUSED_COLUMN"])
        .cascade_sort(vec![("COLUMN", "ASC")])
        .where_(
            vec![
                ("Exp1", Exp {
                    column: "customer_type",
                    operator: "==",
                    compare_with: ExpVal::STR("REGULAR"),
                    compare_as: "COMPARE_AS_TEXT"
                }),
                ("Exp2", Exp {
                    column: "invoice_data",
                    operator: ">",
                    compare_with: ExpVal::STR("2023-12-31 23:59:59"),
                    compare_as: "COMPARE_AS_TEXT"
                }),
                ("Exp3", Exp {
                    column: "invoice_amount",
                    operator: "<",
                    compare_with: ExpVal::STR("1000"),
                    compare_as: "COMPARE_AS_NUMBERS"
                }),
                ("Exp4", Exp {
                    column: "address",
                    operator: "FUZZ_MIN_SCORE_60",
                    compare_with: ExpVal::VEC(vec!["public school"]),
                    compare_as: "COMPARE_AS_TEXT"
                })
            ],
            "Exp1 && (Exp2 || Exp3) && Exp4",
        )
        .print_row_count()
        .save_as("/path/to/modified/file.csv");

#### Chainable Options

    use rgwml::csv_utils::{CsvBuilder, Exp, ExpVal, Piv, CalibConfig};

    CsvBuilder::from_csv("/path/to/your/file1.csv")
    // A. Calibrating an irrugularly formatted file
    .calibrate(
        CalibConfig {
            header_is_at_row: "21",
            rows_range_from: ("23", "*")
        }) // sets the row 21 content as the header, and row 23 to last row content as the data

    // B. Setting and adding headers
    .set_header(vec!["Header1", "Header2", "Header3"])
    .add_column_header("NewColumn1")
    .add_column_headers(vec!["NewColumn2", "NewColumn3"])
    
    // C. Ordering columns
    .order_columns(vec!["Column1", "...", "Column5", "Column2"])
    .order_columns(vec!["...", "Column5", "Column2"])
    .order_columns(vec!["Column1", "Column5", "..."])
    
    // D. Modifying columns
    .drop_columns(vec!["Column1", "Column3"])
    .rename_columns(vec![("Column1", "NewColumn1"), ("Column3", "NewColumn3")])
    
    // E. Adding and modifying rows
    .add_row(vec!["Row1-1", "Row1-2", "Row1-3"])
    .add_rows(vec![vec!["Row1-1", "Row1-2", "Row1-3"], vec!["Row2-1", "Row2-2", "Row2-3"]])
    .remove_duplicates()
    
    // F. Cleaning/ Replacing Cell values
    .trim_all() // Trims white spaces at the beginning and end of all cells in all columns.
    .replace_all(vec!["Column1", "Column2"], vec![("null", ""), ("NA", "-")]) // In specified columns
    .replace_all(vec!["*"], vec![("null", ""), ("NA", "-")]) // In all columns
    
    // G. Limiting and sorting
    .limit(10)
    .cascade_sort(vec![("Column1", "DESC"), ("Column3", "ASC")])
    
    // H. Applying conditional operations
    .where_(
        vec![
            ("Exp1", Exp {
                column: "customer_type",
                operator: "==",
                compare_with: ExpVal::STR("REGULAR"),
                compare_as: "COMPARE_AS_TEXT"
            }),
            ("Exp2", Exp {
                column: "invoice_data",
                operator: ">",
                compare_with: ExpVal::STR("2023-12-31 23:59:59"),
                compare_as: "COMPARE_AS_TEXT"
            }),
            ("Exp3", Exp {
                column: "invoice_amount",
                operator: "<",
                compare_with: ExpVal::STR("1000"),
                compare_as: "COMPARE_AS_NUMBERS"
            }),
            ("Exp4", Exp {
                column: "address",
                operator: "FUZZ_MIN_SCORE_60",
                compare_with: ExpVal::VEC(vec!["public school"]),
                compare_as: "COMPARE_AS_TEXT"
            }),
            ("Exp5", Exp {
                column: "status",
                operator: "CONTAINS",
                compare_with: ExpVal::STR("REJECTED"),
                compare_as: "COMPARE_AS_TEXT"
            }),
            ("Exp6", Exp {
                column: "status",
                operator: "DOES_NOT_CONTAIN",
                compare_with: ExpVal::STR("HAS NOT PAID"),
                compare_as: "COMPARE_AS_TEXT"
            }),
            ("Exp7", Exp {
                column: "status",
                operator: "STARTS_WITH",
                compare_with: ExpVal::STR("VERIFIED"),
                compare_as: "COMPARE_AS_TEXT"
            }),
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6 && Exp7")
    .where_set(
        vec![
            // Same as .where() 
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6 && Exp7",
        "Column10",
        "IS OKAY")

    // I. Analytical Prints for data inspection
    .print_columns()
    .print_row_count()
    .print_first_row()
    .print_last_row()
    .print_rows_range(2,5) // Shows results per a spreadsheet row range
    .print_rows() // Shows results as per a spreadsheet row range
    .print_cells(vec!["Column1", "Column2"])
    .print_unique("column_name")
    .print_freq(vec!["Column1", "Column2"])
    .print_freq_mapped(vec![
            ("Column1", vec![
                ("Delhi", vec!["New Delhi", "Delhi"]),
                ("UP", vec!["Ghaziabad", "Noida"])
            ]),
            ("Column2", vec![("NO_GROUPINGS", vec![])])
        ])
    .print_count_where(
        vec![
            // Same as .where()
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6 && Exp7")

    // J. Grouping Data
    .split_as("ColumnNameToGroupBy", "/output/folder/for/grouped/csv/files/") // Groups data by a specified column and saves each group into a separate CSV file in a given folder

    // K. Basic Set Theory Operations (for the Universe U = {1,2,3,4,5,6,7}, A = {1,2,3} and B = {3,4,5})
    .set_union_with("/path/to/set_b/file.csv", "UNION_TYPE:ALL") // {1,2,3,3,4,5} 
    .set_union_with("/path/to/set_b/file.csv", "UNION_TYPE:ALL_WITHOUT_DUPLICATES") // {1,2,3,4,5}
    .set_intersection_with("/path/to/set_b/file.csv") // {3}
    .set_difference_with("/path/to/set_b/file.csv") // {1,2} i.e. in A but not in B
    .set_symmetric_difference_with("/path/to/set_b/file.csv") // {1,2,4,5} i.e. in either, but not in intersection
    
    // .set_complement_with determines the compliment qua the universe i.e. {4,5,6,7}. Pass an exclusion vector to exclude specific columns of the universe from consideration, or use vec!["INCLUDE_ALL"] to include all columns of the universe.
    .set_complement_with("/path/to/universe_set_u/file.csv", vec!["INCLUDE_ALL"]) 
    .set_complement_with("/path/to/universe_set_u/file.csv", vec!["Column4", "Column5"]) 

    // L. Advanced Set Theory Operations
    .set_union_with("/path/to/table_b.csv", "UNION_TYPE:LEFT_JOIN_AT{{Column1}}") // Left join using "Column1" as the join column.
    .set_union_with("/path/to/table_b.csv", "UNION_TYPE:RIGHT_JOIN_AT{{Column1}}") // Right join using "ID" as the join column.

    // M. Append Derivative Columns
    .append_derived_boolean_column(
        "is_qualified_for_discount",
        vec![
            // Same as .where() 
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6 && Exp7")
    .append_derived_category_column(
        "EXPENSE_RANGE",
        vec![
            (
                "< 1000",
                vec![
                    ("Exp1", Exp {
                        column: "Withdrawal Amt.",
                        operator: "<",
                        compare_with: ExpVal::STR("1000"),
                        compare_as: "COMPARE_AS_NUMBERS"
                    }),
                ],
                "Exp1"
            ),
            (
                "1000-5000",
                vec![
                    ("Exp1", Exp {
                        column: "Withdrawal Amt.",
                        operator: ">=",
                        compare_with: ExpVal::STR("1000"),
                        compare_as: "COMPARE_AS_NUMBERS"
                    }),
                    ("Exp2", Exp {
                        column: "Withdrawal Amt.",
                        operator: "<",
                        compare_with: ExpVal::STR("5000"),
                        compare_as: "COMPARE_AS_NUMBERS"
                    }),
                ],
                "Exp1 && Exp2"
            )
        )
    .append_derived_concatenation_column("NewColumnName", vec!["Column1", " ", "Column2", "@"]) // Items in the vector that are not column names will be concatenated as strings
    .append_fuzzai_analysis_columns(
        "Column1", // Name of column to be analyzed
        "sales_analysis", // Identifier for newly created columns
        vec![
            Train {
                input: "I want my money back",
                output: "refund"
            },
            Train {
                input: "I want a refund immediately",
                output: "refund"
            },
        ],
        "WORD_SPLIT:2", // The minimum length of word combinations that training data is to be broken into
        "WORD_LENGTH_SENSITIVITY:0.8", // Multiplies differences in word length between training data input and the value being analyzed by 0.8
        "GET_BEST:2" // Get the top 2 results, max value is 3
        )
    .split_date_as_appended_category_columns("Column10", "%d/%m/%y") // Appends additional columns splitting a date/timestamp into categorization columns by year, month and week

    // N. Pivot Tables
    .pivot_as(
        "/path/to/save/the/pivot/file/as/csv",
        Piv {
            index_at: "month",
            values_from: "sales",
            operation: "MEDIAN",
            seggregate_by: vec![
                ("is_customer", "AS_BOOLEAN") // Is appended directly as a seggregation column
                ("acquisition_type", "AS_CATEGORY") // The unique values of this column are appended as seggregation columns
            ],
        })

    // O. Save
    .save_as("/path/to/your/file2.csv")

#### Extract Data

These methods return a CsvBuilder object, and hence, can not be subsequently chained.

    CsvBuilder::from_csv("/path/to/your/file1.csv")

    .get_unique("column_name"); // Returns a Vec<String>
    .get("column_name"); // Returns cell content as a String, if the csv has been filtered to single row.
    .get_freq(vec!["Column1", Column2]) // Returns a HashMap where keys are column names and values are vectors of sorted (value, frequency) pairs.
    .get_freq_mapped(vec![
            ("Column1", vec![
                ("Delhi", vec!["New Delhi", "Delhi"]),
                ("UP", vec!["Ghaziabad", "Noida"])
            ]),
            ("Column2", vec![("NO_GROUPINGS", vec![])])
        ])

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
            .save_as("/path/to/daily/sales/report/cache.csv");

        Ok(())
    }

    #[tokio::main]
    async fn main() {
        let cache_path = "/path/to/daily_sales_report.csv";
        let cache_duration_minutes = 1440; // Cache duration set to 1 day

        let result = CsvResultCacher::fetch_async(
            || Box::pin(generate_daily_sales_report()),
            "/path/to/daily/sales/report/cache.csv",
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

The future_for function suits scenarios where you need a moderate level of concurrency. For instance, when each item's processing is relatively straightforward and doesn't require complex shared state or high-throughput parallelism. 

    use rgwml::loop_utils::FutureLoop;

    #[tokio::main()]
    async fn main() {
        let data = vec![1, 2, 3, 4, 5];
        let results = FutureLoop::future_for(data, |num| async move {
            // Perform an async operation
            num * 2
        }).await;
        println!("Results: {:?}", results);
    }

6. License
----------

This project is licensed under the MIT License - see the LICENSE file for details.
