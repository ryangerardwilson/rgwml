// lib.rs
//! # RGWML (an AI, Data Science & Machine Learning Library designed to minimize developer cognitive load)
//!
//! This library simplifies Data Science, Machine Learning, and Artifical Intelligence operations. It's designed to leverage the best features of RUST, in a manner that is graceful, elegant, and ticklishly fun to build upon.
//!
//! ## Modules Overview
//!
//! ### `df_utils`
//! - **Purpose**: Replicate Python Pandas library functionality in Rust.
//! - **Features**: Data manipulation and transformation, filtering, sorting, and aggregating datasets.
//!
//! ### `ai_utils`
//! - **Purpose**: Leverage Rust's concurrency for AI/Graph Theory based analysis.
//! - **Features**: Perform complex data analyses and process neural associations in parallel, harnessing Rust's performance and safety.
//!
//! ### `api_utils`
//! - **Purpose**: Helper functions for making API calls.
//! - **Features**: Simplify the process of sending various HTTP requests and handling responses.
//!
//! ### `csv_utils`
//! - **Purpose**: Functions to analyze and manipulate CSV data.
//! - **Features**: Read, write, and process CSV files with ease, making data analysis more efficient.
//!
//! ## Quick Start
//! - Add the library to your `Cargo.toml`.
//! - Import the modules you need in your Rust application:
//!
//! ## Examples
//!
//! ### df_utils
//!
//! #### Query
//!
//! `Query` struct provides a fluent interface for querying and manipulating data within a `DataFrame`.
//!
//! It supports operations like selecting specific columns, applying conditions to rows, limiting the
//! number of results, filtering rows based on their indices, and performing multi-level sorting using
//! the `cascade_sort` method.
//!
//! Fields
//!
//! - `dataframe`: The DataFrame on which the queries are executed.
//! - `conditions`: A vector of boxed closures that define conditions for filtering rows based on column values.
//! - `index_conditions`: A vector of boxed closures that define conditions for filtering rows based on row indices.
//! - `limit`: An optional limit on the number of rows to return.
//! - `selected_columns`: An optional vector of columns to select in the final result.
//! - `order_by_sequence`: A vector of sorting criteria used for multi-level sorting through `cascade_sort`.
//!
//! Example demonstrating the use of the `Query` struct.
//!
//! In this example, we create a `Query` instance and utilize its various features:
//! - Select specific columns.
//! - Apply conditions on column values.
//! - Filter based on row indices.
//! - Limit the number of results.
//! - Apply multi-level sorting based on specified criteria using `cascade_sort`.
//! - Convert date-time columns to a standardized format.
//!
//! Example
//! ```
//! use std::collections::HashMap;
//! use serde_json::Value;
//! use rgwml::df_utils::{Dataframe, Query};
//!
//! // Assuming DataFrame is a type that holds a collection of data.
//! let df = DataFrame::new(); // Replace with actual DataFrame initialization
//!
//! let result = Query::new(df)
//!     .select(&["column1", "column2"]) // Selecting specific columns
//!     .where_("column1", "==", 42) // Adding a condition based on column value
//!     .where_index_range(0, 10) // Filtering rows based on their index
//!     .limit(5) // Limiting the results to 5 records
//!     .cascade_sort(vec![("column1", "DESC"), ("column2", "ASC")]) // Applying multi-level sorting
//!     .convert_specified_columns_to_lexicographically_comparable_timestamps(&["date_column"])
//!     .execute(); // Executing the query
//!
//! // `result` now contains a DataFrame with the specified columns, conditions, sorting, and limits applied.
//! ```
//!
//! Note: This example assumes the existence of a `DataFrame` type and relevant methods.
//! Replace placeholder code with actual implementations as per your project's context.
//!
//! #### Grouper
//!
//! A utility for grouping rows in a DataFrame based on a specified key.
//!
//! `Grouper` provides a way to categorize and segment data within a DataFrame,
//! where the DataFrame is a collection of rows, and each row is a `HashMap<String, Value>`.
//! It simplifies the process of aggregating, analyzing, or further manipulating
//! data based on grouped criteria.
//!
//! Example
//!
//! ```
//! use std::collections::HashMap;
//! use rgwml::df_utils::{Grouper, DataFrame, convert_json_string_to_dataframe};
//!
//! let json_data = r#"[{"category": "Fruit", "item": "Apple"}, {"category": "Fruit", "item": "Banana"}, {"category": "Vegetable", "item": "Carrot"}]"#;
//! let df = convert_json_string_to_dataframe(json_data).unwrap();
//!
//! let grouper = Grouper::new(&df);
//! let grouped_dfs = grouper.group_by("category");
//!
//! // `grouped_dfs` will now contain two grouped DataFrames, one for each category (`Fruit` and `Vegetable`).
//! ```
//!
//! #### DataFrameCacher
//!
//! A utility designed for caching and retrieving data stored in a structured format known as `DataFrame`. It shines in scenarios where data generation can be time-consuming, such as fetching data from external sources or performing resource-intensive computations.
//!
//! Usage
//!
//! To make the most of the `DataFrameCacher`, follow these steps:
//!
//! 1. **Create a data generator function**: Begin by creating a data generator function that returns a `Future` producing a `Result<DataFrame, Box<dyn Error>>`. This function will be responsible for generating the data you want to cache.
//!
//! 2. **Instantiate a `DataFrameCacher` with the `fetch_async` method**: Once you have your data generator function, you can create an instance of `DataFrameCacher` by providing the data generator function, cache path, and cache duration. If the data is still valid in the cache, it will be retrieved from there. Otherwise, the data generator function will be invoked to obtain fresh data, which will then be cached for future use.
//!
//! Example
//!
//! Below is an example demonstrating the first step of creating a data generator function:
//!
//! ```
//! use rgwml::df_utils::{DataFrame, DataFrameCacher};
//!
//! // Define your asynchronous data generation function here
//! async fn generate_my_data() -> Result<DataFrame, Box<dyn std::error::Error>> {
//!     // Implement your data generation logic here
//!     Ok(vec![])
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//!     let df = DataFrameCacher::fetch_async(
//!         || Box::pin(generate_my_data()), // Data generator function
//!         "/path/to/your/data.json", // Cache path
//!         60, // Cache duration in minutes
//!     ).await?;
//!
//!     dbg!(df);
//!
//!     Ok(())
//! }
//! ```
//!
//! Note
//!
//! The use of `|| Box` in the later example is essential. It allows you to encapsulate your data
//! generation function within a closure and a `Box`. This is required because `DataFrameCacher`
//! expects the data generator function to have a `'static` lifetime. Closures capture their
//! environment, so by using `|| Box`, you ensure that both the closure and the function it
//! captures can be moved into `DataFrameCacher`, satisfying the necessary lifetime constraints.
//!
//! ### ai_utils
//!
//! Dive into the world of AI with `fuzzai`, an asynchronous function that processes neural associations in parallel. Imagine analyzing the neural network's decision-making process in a world where AI has developed a fondness for classic video games!
//!
//! ```rust
//! use rgwml::ai_utils::{NeuralAssociation2D, fuzzai};
//!
//! #[tokio::main]
//! async fn main() {
//!     let neural_associations = vec![
//!         NeuralAssociation2D {
//!             input: "Pac-Man starts",
//!             output: "Collect dots, avoid ghosts",
//!         },
//!         NeuralAssociation2D {
//!             input: "Ghosts in scatter mode",
//!             output: "Focus on dots in corners",
//!         },
//!         // ... more associations ...
//!     ];
//!     let result = fuzzai(
//!         &neural_associations,
//!         "Just started Pac-Man game. What to do?",
//!         "Retro Gaming Analysis",
//!         SplitUpto::WordSetLength(3),
//!         ShowComplications::True,
//!         WordLengthSensitivity::Coefficient(0.25)
//!     ).await.expect("AI should understand Pac-Man!");
//!     println!("AI's take on Pac-Man: {}", result);
//! }
//! ```
//!
//! ### api_utils
//!
//! This module features the APICallBuilder a fluent interface to build API requests with support for method chaining. It simplifies the process by allowing you to specify both headers and payload as `serde_json::Value`. This approach is convenient when dealing with JSON data, making it easy to construct requests dynamically. If caching is enabled, responses are stored and reused for subsequent requests made within the specified cache duration.
//!
//! Example 1: Without Headers
//! ```
//! use serde_json::json;
//! use rgwml::api_utils::ApiCallBuilder;
//!
//! #[tokio::main]
//! async fn main() {
//!     let method = "POST"; // Or "GET", "PUT", "DELETE"
//!     let url = "http://example.com/api/submit";
//!     let payload = json!({
//!         "field1": "Hello",
//!         "field2": 123
//!     });
//!     let response = ApiCallBuilder::call(
//!             method,
//!             url,
//!             None, // No custom headers
//!             Some(payload)
//!         )
//!         .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
//!         .execute()
//!         .await
//!         .unwrap();
//!
//!     println!("Response from server: {}", response);
//! }
//! ```
//!
//! Example 2: With Headers
//! ```
//! use reqwest::Method;
//! use serde_json::json;
//! use rgwml::api_utils::ApiCallBuilder;
//!
//! #[tokio::main]
//! async fn main() {
//!     let method = "POST"; // Or "GET", "PUT", "DELETE"
//!     let url = "http://example.com/api/submit";
//!     let headers = json!({
//!         "Content-Type": "application/json",
//!         "Authorization": "Bearer your_token_here"
//!     });
//!     let payload = json!({
//!         "field1": "Hello",
//!         "field2": 123
//!     });
//!     let response = ApiCallBuilder::call(
//!             method,
//!             url,
//!             Some(headers), // Custom headers
//!             Some(payload)
//!         )
//!         .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
//!         .execute()
//!         .await
//!         .unwrap();
//!
//!     println!("Response from server: {}", response);
//! }
//! ```
//!
//! These examples demonstrate how to use the ApiCallBuilder with and without custom headers. Since the headers and payload are specified as `serde_json::Value`, it offers flexibility in constructing various types of requests.
//!
//! Note: Be cautious when caching POST requests, as they typically send unique data each time. Caching is most effective when the same request is likely to yield the same response.
//!
//! ### csv_utils
//!
//! This module features the CsvBuilder, a fluent interface for creating and writing to CSV files.
//! ```
//! use rgwml::csv_utils::CsvBuilder;
//!
//! let result = CsvBuilder::new("/path/to/your/file.csv")
//!     .set_header(&["Column1", "Column2", "Column3"])
//!     .add_row(&["Row1-1", "Row1-2", "Row1-3"])
//!     .add_rows(&[&["Row2-1", "Row2-2", "Row2-3"], &["Row3-1", "Row3-2", "Row3-3"]]);
//! ```
//!
//! This example demonstrates creating a new CSV file, setting its header, adding individual rows, and a collection of rows. The builder pattern allows for these methods to be chained for ease of use.
//!
//! ## License
//!
//! This project is licensed under the MIT License - see the LICENSE file for details.

pub mod ai_utils;
pub mod api_utils;
pub mod csv_utils;
pub mod df_utils;
