// lib.rs
//! # RGWML (an AI, Data Science & Machine Learning Library designed to minimize developer cognitive load)
//!
//! This library simplifies Data Science, Machine Learning, and Artifical Intelligence operations. It's designed to leverage the best features of RUST, in a manner that is graceful, elegant, and ticklishly fun to build upon.
//!
//! ## Modules Overview
//!
//! ### `csv_utils`
//!   - **Purpose**: A Comprehensive Toolkit for CSV File Management, in AI/ML pipelines. 
//!   - **Features**: `csv_utils` offers a powerful suite of tools designed for efficient and flexible handling of CSV files. Key components include: 
//!     - **CsvBuilder**: A versatile builder for creating and manipulating CSV files, facilitating:- 
//!       - **Easy Initialization**: Start with a new CSV or load from an existing file.
//!       - **Custom Headers and Rows**: Set custom headers and add rows effortlessly.
//!       - **Advanced Data Manipulation**: Rename, drop, and reorder columns, sort data, and apply complex filters like fuzzy matching and timestamp comparisons.
//!       - **Chainable Methods**: Combine multiple operations in a fluent and readable manner.
//!       - **Data Analysis Aids**: Count rows, print specific rows, ranges, or unique values for quick analysis.
//!       - **Flexible Saving Options**: Save your modified CSV to a desired path.
//!     - **CsvResultCacher**: Cache the results of CSV file generation for future use.
//!     - **CsvConverter**: Convert various data structures, like JSON, into CSV format.
//!
//! ### `ai_utils`
//! - **Purpose**: Leverage Rust's concurrency for AI/Graph Theory-based analysis.
//! - **Features**:
//!   - Conduct complex data analyses and process neural networks in parallel.
//!   - Utilize Rust's performance and safety features.
//!   - Work directly with CSV files, with an elegant syntax, for model training.
//!
//! ### `api_utils`
//! - **Purpose**: Gracefully make and cache API calls.
//! - **Features**:
//!   - **ApiCallBuilder**: Make and cache API calls effortlessly, and manage cached data for efficient API usage.
//!
//! ### `loop_utils`
//! - **Purpose**: Simplify asynchronous operations in loops.
//! - **Features**:
//!   - **FutureLoop**: Handle multiple tasks simultaneously when working with lists or collections, while working with a fluent interface.
//!
//! ## csv_utils
//!
//! The `csv_utils` module encompasses a set of utilities designed to simplify various tasks associated with CSV files. These utilities include the `CsvBuilder` for creating and managing CSV files, the `CsvConverter` for transforming JSON data into CSV format, and the `CsvResultCacher` for efficient data caching and retrieval. Each utility is tailored to enhance productivity and ease in handling CSV data in different scenarios.
//! - CsvBuilder: Offers a fluent interface for creating, analyzing, and saving CSV files. It simplifies interactions with CSV data, whether starting from scratch, modifying existing files, etc.
//! - CsvConverter: Provides a method for converting JSON data into CSV format. This utility is particularly useful for processing and saving JSON API responses as CSV files, offering a straightforward approach to data conversion. The `CsvConverter` simplifies the process of converting JSON data into a CSV format. This is particularly useful for scenarios where data is received in JSON format from an API and needs to be transformed into a more accessible and readable CSV file. To use `CsvConverter`, simply call the `from_json` method with the JSON data and the desired output file path as arguments.
//! - CsvResultCacher: Uses a data generator function to create or fetch data, saves it to a specified path, and keeps it for a set duration. This helps avoid unnecessary data regeneration. Imagine you have a CSV file that logs daily temperatures. You don't want to generate this file every time you access it, especially if the data doesn't change much during the day. Here's how you can use CsvResultCacher:
//!
//! Easily print example synatax relating to this feature in your workflow.
//!
//!```
//!     use rgwml::csv_utils::{CsvBuilder, CsvConverter, CsvResultCacher};
//!     use std::collections::HashMap;
//!
//!     #[tokio::main]
//!     async fn main() {
//!         let _ = CsvBuilder::get_docs();
//!         let _ = CsvConverter::get_docs();
//!         let _ = CsvResultCacher::get_docs();
//!         std::process::exit(1);
//!     }
//! ```
//!
//! ## ai_utils
//!
//! This library provides simple AI utilities for neural association analysis. It focuses on processing and analyzing data within neural networks, with an emphasis on understanding AI decision-making processes and text analysis, optimized for a parallel computing environment.
//!
//! Features
//!
//!- **Direct CSV Input**: Utilize CSV file paths directly, specifying input and output column names, to facilitate neural association analysis.
//!- **Parallel Processing**: Leverage parallel computing to efficiently analyze neural associations, gaining insights into AI decision-making processes.
//!
//! Example
//!```
//!     use rgwml::ai_utils::{fuzzai, SplitUpto, ShowComplications, WordLengthSensitivity};
//!     use std::error::Error;
//!     
//!     #[tokio::main]
//!     async fn main() {
//!         // Call the fuzzai function with CSV file path
//!         let fuzzai_result = fuzzai(
//!             "path/to/your/model/training/csv/file.csv",
//!             "model_training_input_column_name",
//!             "model_training_output_column_name",
//!             "your text to be analyzed against the training data model",
//!             "your task description: clustering customer complaints",
//!             SplitUpto::WordSetLength(2), // Set the minimum word length of combination value to split the training input data during the analysis
//!             ShowComplications::False, // Set to True to see inner workings of the model
//!             WordLengthSensitivity::Coefficient(0.2), // Set to Coefficient::None to disregard differences in the word length of the training input and the text being analyzed; Increase the coefficient to give higher weightage to matches with similar word length
//!             ).await.expect("Analysis should succeed");
//!
//!         dbg!(fuzzai_result);
//!     }
//!
//! ## api_utils
//!
//! Easily print example synatax relating to this feature in your workflow.
//!
//! ```
//!     use serde_json::json;
//!     use rgwml::api_utils::ApiCallBuilder;
//!     use std::collections::HashMap;
//!
//!     #[tokio::main]
//!     async fn main() {
//!         let _ = ApiCallBuilder::get_docs();
//!         std::process::exit(1);
//!     }
//! ```
//!
//! ## License
//!
//! This project is licensed under the MIT License - see the LICENSE file for details.

pub mod ai_utils;
pub mod api_utils;
pub mod csv_utils;
pub mod loop_utils;
