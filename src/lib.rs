// lib.rs


//! # RGWML (the Ryan Gerard Wilson AI, Data Science & Machine Learning Crates)
//!
//!  This Rust library simplifies data science, ml, and ai operations, including but not limited to the process of creating and handling CSV files. It's designed to be easy to use, and eliminate the need to ever use a GUI tool.
//!
//!
//! ## Modules Overview
//!
//! ### `api_utils`
//! - **Purpose**: Helper functions for making API calls.
//! - **Features**: Simplify the process of sending various HTTP requests and handling responses.
//!
//! ### `csv_utils`
//! - **Purpose**: Functions to analyze and manipulate CSV data.
//! - **Features**: Read, write, and process CSV files with ease, making data analysis more efficient.
//!
//! ### `df_utils`
//! - **Purpose**: Replicate Python Pandas library functionality in Rust.
//! - **Features**: Data manipulation and transformation, filtering, sorting, and aggregating datasets.
//!
//! ### `ai_utils`
//! - **Purpose**: Leverage Rust's concurrency for AI/Graph Theory based analysis.
//! - **Features**: Perform complex data analyses and process neural associations in parallel, harnessing Rust's performance and safety.
//!
//! ## Quick Start
//! - Add the library to your `Cargo.toml`.
//! - Import the modules you need in your Rust application:
//!
//! ## Examples
//!
//! ### ai_utils
//!
//! Dive into the world of AI with `fuzzai`, an asynchronous function that processes neural associations in parallel. Imagine analyzing the neural network's decision-making process in a world where AI has developed a fondness for classic video games!
//! 
//! ```rust
//! use rgwml::ai_utils::fuzzai;
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
//!         WordLengthSensitivityCoefficient::Coefficient(1.0)
//!     ).await.expect("AI should understand Pac-Man!");
//!     println!("AI's take on Pac-Man: {}", result);
//! }
//! ```
//!
//! ### api_utils
//! 
//! Send a request to an API that tells you the current weather on Mars! This function is perfect for interplanetary weather enthusiasts or anyone curious about the Red Planet's climate.
//! 
//! ```
//! use reqwest::Method;
//! use rgwml::api_utils::call_api;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mars_weather_api = "https://api.marsweather.app";
//!     let response = call_api(Method::GET, mars_weather_api, HeaderOption::None, None::<()>).await
//!         .expect("Failed to get Mars weather. Are the satellites okay?");
//!     println!("Current weather on Mars: {}", response);
//! }
//! ```
//!
//! ### csv_utils
//! 
//! Create a CSV file listing the top 5 fictional spaceships. This example is perfect for sci-fi fans who like to keep track of their favorite spacecraft.
//! 
//! ```
//! use rgwml::csv_utils::create_csv;
//!
//! fn main() {
//!     let spaceships = ["Millennium Falcon", "Starship Enterprise", "Serenity", "Battlestar Galactica", "TARDIS"];
//!     let result = create_csv("/path/to/spaceships.csv", &spaceships);
//!     assert!(result.is_ok(), "Failed to save the coolest spaceships. Try again!");
//! }
//! ```
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
//! For more detailed examples, refer to the documentation of each module.
//!
//! ## License
//!
//! This project is licensed under the MIT License - see the LICENSE file for details.

pub mod ai_utils;
pub mod api_utils;
pub mod csv_utils;
pub mod df_utils;


