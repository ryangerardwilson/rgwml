# RGWML (an AI, Data Science & Machine Learning Library designed to minimize developer cognitive load)

This library simplifies Data Science, Machine Learning, and Artifical Intelligence operations. It's designed to leverage the best features of RUST, in a manner that is easy to use, and fun to build upon.

A powerful and user-friendly toolkit designed to simplify operations in data science, machine learning, and artificial intelligence. With a focus on eliminating the need for GUI tools, it streamlines tasks such as CSV file handling and API interactions, while also replicating functionalities akin to Python's Pandas library. Additionally, it leverages Rust's native concurrency features for efficient AI and graph theory-based data analysis.

## Modules Overview

### `df_utils`
- **Purpose**: Replicate Python Pandas library functionality in Rust.
- **Features**: Data manipulation and transformation, filtering, sorting, and aggregating datasets.

### `ai_utils`
- **Purpose**: Leverage Rust's concurrency for AI/Graph Theory based analysis.
- **Features**: Perform complex data analyses and process neural associations in parallel, harnessing Rust's performance and safety.

### `api_utils`
- **Purpose**: Helper functions for making API calls.
- **Features**: Simplify the process of sending various HTTP requests and handling responses.

### `csv_utils`
- **Purpose**: Functions to analyze and manipulate CSV data.
- **Features**: Read, write, and process CSV files with ease, making data analysis more efficient.

## Quick Start

1. Add the library to your `Cargo.toml`.
2. Import the modules you need in your Rust application:

```rust
use rgwml::{api_utils, csv_utils, df_utils, ai_utils};
