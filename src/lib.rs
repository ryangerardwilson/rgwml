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
//! Use DataFrame utilities to analyze a list of fictional planets and their characteristics. Perfect for a galactic explorer or a daydreaming astronomer.
//! 
//! ```
//! use rgwml::df_utils::{convert_json_string_to_dataframe, Query};
//! use serde_json::Value;
//!
//! fn main() {
//!     let json_data = r#"[{"name": "Tatooine", "population": 200000}, {"name": "Vulcan", "population": 6000000}]"#;
//!     let df = convert_json_string_to_dataframe(json_data).expect("Parsing failed. Are these planets real?");
//!     let habitable_planets = Query::new(df)
//!         .where_("population", ">", Value::Number(1000000.into()))
//!         .execute();
//!     println!("Habitable planets for humans: {:?}", habitable_planets);
//! }
//! ```
//!
//! Imagine converting data about mythical creatures into a JSON format. Here's how you can do it:
//! ```
//! use rgwml::df_utils::{data_frame_to_value_array};
//! use serde_json::Value;
//! use std::collections::HashMap;
//!
//! fn main() {
//!     let mythical_creatures = vec![
//!         HashMap::from([
//!             ("name".to_string(), Value::String("Dragon".to_string())),
//!             ("element".to_string(), Value::String("Fire".to_string())),
//!         ]),
//!         // ... add more creatures ...
//!     ];
//!
//!     let value_array = data_frame_to_value_array(mythical_creatures);
//!     println!("Mythical Creatures in JSON format: {:?}", value_array);
//! }
//! ```
//! Here we extract unique types of magical artifacts from a DataFrame:
//! ```
//! use rgwml::df_utils::{convert_json_string_to_dataframe, get_unique_values};
//!
//! fn main() {
//!     let json_artifacts = r#"[{"type": "Amulet", "power": 50}, {"type": "Ring", "power": 30}, {"type": "Amulet", "power": 45}]"#;
//!     let artifacts_df = convert_json_string_to_dataframe(json_artifacts).unwrap();
//!     let unique_artifacts_df = get_unique_values(&artifacts_df, "type");
//!     println!("Unique Artifacts: {:?}", unique_artifacts_df);
//! }
//! ```
//! Filtering enchanted forests by their magic level and limiting the result:
//! ```
//! use rgwml::df_utils::{convert_json_string_to_dataframe, Query};
//! use serde_json::Value;
//!
//! fn main() {
//!     let json_forests = r#"[{"name": "Emerald Woods", "magic_level": 80}, {"name": "Silvermist Forest", "magic_level": 95}, {"name": "Darkshade Woods", "magic_level": 40}]"#;
//!     let forests_df = convert_json_string_to_dataframe(json_forests).unwrap();
//!     
//!     let high_magic_forests = Query::new(forests_df)
//!         .where_("magic_level", ">", Value::Number(50.into()))
//!         .limit(3)
//!         .execute();
//!     println!("High Magic Level Forests: {:?}", high_magic_forests);
//! }
//! ```
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
//! use reqwest::Method;
//! use serde_json::json;
//! use rgwml::api_utils::ApiCallBuilder;
//!
//! #[tokio::main]
//! async fn main() {
//!     let url = "http://example.com/api/submit";
//!     let payload = json!({
//!         "field1": "Hello",
//!         "field2": 123
//!     });
//!     let response = ApiCallBuilder::call(
//!             Method::POST,
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
//!             Method::POST,
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

pub mod df_utils;
pub mod ai_utils;
pub mod api_utils;
pub mod csv_utils;


