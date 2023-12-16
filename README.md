# RGWML (the Ryan Gerard Wilson AI, Data Science & Machine Learning Crates)

This library simplifies data science, ml, and ai operations, including but not limited to the process of creating and handling CSV files. It's designed to be easy to use, and eliminate the need to ever use a GUI tool.

This Rust library is a powerful and user-friendly toolkit designed to simplify operations in data science, machine learning, and artificial intelligence. With a focus on eliminating the need for GUI tools, it streamlines tasks such as CSV file handling and API interactions, while also replicating functionalities akin to Python's Pandas library. Additionally, it leverages Rust's native concurrency features for efficient AI and graph theory-based data analysis.

## Modules Overview

### `api_utils`
- **Purpose**: Helper functions for making API calls.
- **Features**: Simplify the process of sending various HTTP requests and handling responses.

### `csv_utils`
- **Purpose**: Functions to analyze and manipulate CSV data.
- **Features**: Read, write, and process CSV files with ease, making data analysis more efficient.

### `df_utils`
- **Purpose**: Replicate Python Pandas library functionality in Rust.
- **Features**: Data manipulation and transformation, filtering, sorting, and aggregating datasets.

### `ai_utils`
- **Purpose**: Leverage Rust's concurrency for AI/Graph Theory based analysis.
- **Features**: Perform complex data analyses and process neural associations in parallel, harnessing Rust's performance and safety.

## Quick Start

1. Add the library to your `Cargo.toml`.
2. Import the modules you need in your Rust application:

```rust
use rust_data_science_ai_lib::{api_utils, csv_utils, df_utils, ai_utils};
