// csv_utils.rs
use crate::ai_utils::{
    create_openai_batch, get_openai_analysis_json, retrieve_openai_batch, upload_file_to_openai,
};
use crate::clustering_utils::{ClusteringConfig, ClusteringConnect};
use crate::dask_utils::{
    DaskCleanerConfig, DaskConnect, DaskDifferentiatorConfig, DaskFreqCascadingConfig,
    DaskFreqLinearConfig, DaskGrouperConfig, DaskIntersectorConfig, DaskJoinerConfig,
    DaskPivoterConfig,
};
use crate::db_utils::DbConnect;
use crate::dc_utils::{DataContainer, DcConnectConfig};
use crate::public_url_utils::{PublicUrlConnect, PublicUrlConnectConfig};
use crate::xgb_utils::{XgbConfig, XgbConnect};
use anyhow::Result as AnyhowResult;
use calamine::{open_workbook, Reader, Xls, Xlsx};
use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, TimeZone, Timelike,
    Utc,
};
use csv::Writer;
use futures::executor::block_on;
use futures::future::join_all;
use fuzzywuzzy::fuzz;
use memmap::MmapOptions;
use rand::{seq::SliceRandom, thread_rng};
use rayon::prelude::*;
use regex::Regex;
use serde_json::{json, Map, Value};
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::linear::linear_regression::{
    LinearRegression, LinearRegressionParameters, LinearRegressionSolverName,
};
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::Debug;
use std::fs::metadata;
use std::fs::read_dir;
use std::fs::{remove_file, File};
use std::io;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::mem;
use std::process::Command;
use std::str::FromStr;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tempfile::NamedTempFile;
use uuid::Uuid;

/// A utility struct for converting JSON data to CSV format.
pub struct CsvConverter;

/// Represents a Calibration specification indicating for a mal-formatted csv file, what to
/// consider as the header and the data range.
pub struct CalibConfig {
    pub header_is_at_row: String,
    pub rows_range_from: (String, String),
}

/// Represents a CsvBuilder object. This struct allows you to specify headers, corresponding data, a limit on how much data to consider for subsequent manipulations, as well as an internal error handler.
#[derive(Debug)]
pub struct CsvBuilder {
    headers: Vec<String>,
    data: Vec<Vec<String>>,
    limit: Option<usize>,
    error: Option<Box<dyn Error>>,
}

/// Represents a Training instance object. This structs allows you specify input output pairs for supervised learning algorithms in this library.
#[derive(Debug)]
pub struct Train {
    pub input: String,
    pub output: String,
}

/// Represents an Expression instance object. This struct allows you specify an expression to compare column values of tabulated csv data.
#[derive(Debug, Clone)]
pub struct Exp {
    pub column: String,
    pub operator: String,
    pub compare_with: ExpVal,
    pub compare_as: String,
}

/// Represents the data structure type of the compare_with element of the Exp struct. This allows you pass in both String and `Vec<String>` values to compare csv data with.
#[derive(Debug, Clone)]
pub enum ExpVal {
    STR(String),
    VEC(Vec<String>),
}

impl CsvConverter {
    /// Converts JSON data to a CSV file.
    ///
    /// # Arguments
    ///
    /// * `json_data` - A string slice that holds the JSON data.
    /// * `file_path` - The path where the CSV file will be saved.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvConverter;
    /// use std::fs;
    /// use tempfile::NamedTempFile;
    ///
    /// let json_data = r#"[
    ///     {"name": "Alice", "age": "30"},
    ///     {"name": "Bob", "age": "25"}
    /// ]"#;
    ///
    /// let mut file = NamedTempFile::new().unwrap();
    /// let file_path = file.path().to_str().unwrap();
    ///
    /// assert!(CsvConverter::from_json(json_data, file_path).is_ok());
    /// assert!(fs::metadata(file_path).is_ok()); // Check if file has been created
    ///
    /// let contents = fs::read_to_string(file_path).unwrap();
    /// println!("CSV Content:\n{}", contents); // Print the entire content of the CSV file
    ///
    /// // Define multiple acceptable headers and their corresponding rows
    /// let acceptable_headers = ["name,age", "age,name"];
    /// let expected_data = [
    ///     ["Alice,30", "Bob,25"],   // Corresponds to "name,age"
    ///     ["30,Alice", "25,Bob"]    // Corresponds to "age,name"
    /// ];
    ///
    /// let lines: Vec<_> = contents.lines().collect(); // Split contents into lines
    /// let header = lines[0]; // Assuming the first line is always the header
    ///
    /// // Find which set of expected data corresponds to the detected header
    /// let data_index = acceptable_headers.iter().position(|&h| h == header).unwrap_or_else(|| {
    ///     panic!("Unexpected header format: {}", header);
    /// });
    ///
    /// // Check each expected data line against the contents
    /// for &expected_line in &expected_data[data_index] {
    ///     assert!(lines.contains(&expected_line), "Expected line not found: {}", expected_line);
    /// }
    /// ```
    pub fn from_json(json_data: &str, file_path: &str) -> AnyhowResult<()> {
        let data: Value = serde_json::from_str(json_data)?;

        let file = File::create(file_path)?;
        let mut wtr = csv::Writer::from_writer(file);

        if let Value::Array(items) = data {
            let keys: Option<Vec<String>> = items.first().and_then(|item| match item {
                Value::Object(map) => Some(map.keys().cloned().collect()),
                _ => None,
            });

            if let Some(keys) = keys {
                wtr.write_record(&keys)?;

                // Collect results first
                let results: Vec<_> = items
                    .par_iter()
                    .filter_map(|item| {
                        if let Value::Object(map) = item {
                            let row: Vec<String> = keys
                                .iter()
                                .map(|key| {
                                    map.get(key).map_or_else(
                                        || "".to_string(),
                                        |v| match v {
                                            Value::String(s) => s.replace("\"", ""),
                                            _ => v.to_string().replace("\"", ""),
                                        },
                                    )
                                })
                                .collect();

                            Some(row)
                        } else {
                            None
                        }
                    })
                    .collect();

                // Write all rows sequentially
                for row in results {
                    wtr.write_record(&row)?;
                }
            }
        }

        wtr.flush()?;
        Ok(())
    }
}

/// Defines the trait for comparison values
pub trait CompareValue {
    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool;
}

/// Implements CompareValue for a single string reference
impl CompareValue for String {
    /// Applies a comparison operation to a cell value based on the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `cell_value` - A string slice that holds the value to compare against.
    /// * `operation` - The operation to perform ("==", "!=", "CONTAINS", etc.).
    /// * `compare_as` - The type of comparison ("TEXT", "NUMBERS", "TIMESTAMPS").
    ///
    /// ```
    /// use rgwml::csv_utils::CompareValue;
    ///
    /// let comparator = "example".to_string();
    ///
    /// // Text comparison
    /// assert_eq!(comparator.apply("example", "==", "TEXT"), true);
    /// assert_eq!(comparator.apply("example", "!=", "TEXT"), false);
    /// assert_eq!(comparator.apply("ample", "CONTAINS", "TEXT"), false);
    /// assert_eq!(comparator.apply("example", "STARTS_WITH", "TEXT"), true);
    /// assert_eq!(comparator.apply("xample", "DOES_NOT_START_WITH", "TEXT"), true);
    /// assert_eq!(comparator.apply("xample", "DOES_NOT_CONTAIN", "TEXT"), true);
    ///
    /// // Number comparison
    /// let comparator = "5".to_string();
    /// assert_eq!(comparator.apply("5", "==", "NUMBERS"), true);
    /// assert_eq!(comparator.apply("4", ">", "NUMBERS"), false);
    /// assert_eq!(comparator.apply("6", "<", "NUMBERS"), false);
    /// assert_eq!(comparator.apply("5", ">=", "NUMBERS"), true);
    /// assert_eq!(comparator.apply("5", "<=", "NUMBERS"), true);
    /// assert_eq!(comparator.apply("6", "!=", "NUMBERS"), true);
    ///
    /// // Timestamp comparison
    /// let comparator = "2024-01-01T00:00:00Z".to_string();
    /// assert_eq!(comparator.apply("2024-01-01T00:00:00Z", "==", "TIMESTAMPS"), true);
    /// assert_eq!(comparator.apply("2024-01-02T00:00:00Z", "<", "TIMESTAMPS"), false);
    /// assert_eq!(comparator.apply("2023-12-31T23:59:59Z", ">", "TIMESTAMPS"), false);
    /// assert_eq!(comparator.apply("2024-01-01T00:00:00Z", ">=", "TIMESTAMPS"), true);
    /// assert_eq!(comparator.apply("2024-01-01T00:00:00Z", "<=", "TIMESTAMPS"), true);
    /// assert_eq!(comparator.apply("2023-12-31T23:59:59Z", "!=", "TIMESTAMPS"), true);
    ///
    /// // Incorrect operation handling
    /// assert_eq!(comparator.apply("5", "?", "NUMBERS"), false);
    /// ```

    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool {
        /// Compares `cell_value` with `value` based on the `operation` specified for text-based attributes.
        fn apply_text(value: &String, cell_value: &str, operation: &str) -> bool {
            match operation {
                "==" => cell_value == value,
                "!=" => cell_value != value,
                "CONTAINS" => cell_value.contains(value),
                "STARTS_WITH" => cell_value.starts_with(value),
                "DOES_NOT_CONTAIN" => !cell_value.contains(value),
                "DOES_NOT_START_WITH" => !cell_value.starts_with(value),
                _ => false,
            }
        }

        /// Compares `cell_value` with `value` as floating point numbers based on the `operation` specified, treating empty string as zero.
        fn apply_numbers(value: &String, cell_value: &str, operation: &str) -> bool {
            let cell_value = cell_value.trim();
            let cell_value = if cell_value.is_empty() {
                "0"
            } else {
                cell_value
            };
            match (cell_value.parse::<f64>(), value.parse::<f64>()) {
                (Ok(n1), Ok(n2)) => match operation {
                    "==" => n1 == n2,
                    ">" => n1 > n2,
                    "<" => n1 < n2,
                    ">=" => n1 >= n2,
                    "<=" => n1 <= n2,
                    "!=" => n1 != n2,
                    _ => false,
                },
                _ => false,
            }
        }

        /// Compares timestamps derived from `cell_value` and `value` based on the `operation`, using custom parsing logic from `CsvBuilder`.
        fn apply_timestamps(value: &String, cell_value: &str, operation: &str) -> bool {
            /// Parses a timestamp from a string using multiple predefined date formats, returning `Result<NaiveDateTime, String>` if successful or an error message.
            fn parse_timestamp(time_str: &str) -> Result<NaiveDateTime, String> {
                let formats = vec![
                    "%Y-%m-%d %H:%M:%S",
                    "%+",
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%d",
                    "%m/%d/%Y %I:%M:%S %p",
                    // Add other formats as needed
                ];

                let parsed_date = formats
                    .iter()
                    .find_map(|&format| NaiveDateTime::parse_from_str(time_str, format).ok())
                    .or_else(|| {
                        DateTime::parse_from_rfc2822(time_str)
                            .map(|dt| dt.naive_local())
                            .ok()
                    })
                    .or_else(|| {
                        DateTime::parse_from_rfc3339(time_str)
                            .map(|dt| dt.naive_local())
                            .ok()
                    });

                match parsed_date {
                    Some(date) => Ok(date),
                    None => Err(format!("Unable to parse '{}' as a timestamp", time_str)),
                }
            }

            let row_date = parse_timestamp(cell_value);
            let compare_date = parse_timestamp(value);

            match (row_date, compare_date) {
                (Ok(r), Ok(c)) => match operation {
                    "==" => r == c,
                    ">" => r > c,
                    "<" => r < c,
                    ">=" => r >= c,
                    "<=" => r <= c,
                    "!=" => r != c,
                    _ => false,
                },
                _ => false,
            }
        }

        match compare_as {
            "TEXT" => apply_text(self, cell_value, operation),
            "NUMBERS" => apply_numbers(self, cell_value, operation),
            "TIMESTAMPS" => apply_timestamps(self, cell_value, operation),
            _ => false,
        }
    }
}

/// Implements CompareValue for a Vec<&str> reference
impl CompareValue for Vec<String> {
    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool {
        if operation.starts_with("FUZZ_MIN_SCORE_") && compare_as == "TEXT" {
            // Extract the score threshold from the operation string
            let score_threshold: i32 = operation["FUZZ_MIN_SCORE_".len()..].parse().unwrap_or(70);
            let re = Regex::new(r"[^a-zA-Z\s]").unwrap();
            let only_alpha = re.replace_all(cell_value, "");
            let cleaned_cell_value = only_alpha
                .trim()
                .split_whitespace()
                .collect::<Vec<&str>>()
                .join(" ");
            let words: Vec<&str> = cleaned_cell_value.split_whitespace().collect();
            let mut all_scores = vec![];

            for value in self.iter() {
                let value_word_count = value.split_whitespace().count();
                let mut futures = vec![];

                for len in value_word_count..=words.len() {
                    for combination in words.windows(len) {
                        let combined = combination.join(" ");
                        let value_clone = value.to_string();
                        futures.push(async move { fuzz::ratio(&combined, &value_clone) });
                    }
                }

                let scores = block_on(join_all(futures));
                all_scores.extend(scores);
            }

            let max_score = *all_scores.iter().max().unwrap_or(&0);
            //dbg!(&max_score);
            i32::from(max_score) >= score_threshold
        } else {
            false
        }
    }
}

impl CsvBuilder {
    /// Creates a new, empty `CsvBuilder`.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new();
    ///
    /// // Initially, there are no headers or data
    /// assert!(builder.get_headers().is_none());
    /// assert!(builder.get_data().is_none());
    pub fn new() -> Self {
        CsvBuilder {
            headers: Vec::new(),
            data: Vec::new(),
            limit: None,
            error: None,
        }
    }

    /// Reads data from a CSV file at the specified `file_path` and returns a `CsvBuilder`.
    ///
    /// ## Valid CSV file
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::io::Write;
    /// use csv::Writer;
    ///
    /// let mut tmp_file = tempfile::Builder::new()
    ///     .prefix("csv_test")
    ///     .suffix(".csv")
    ///     .tempfile()
    ///     .expect("failed to create temporary file");
    ///
    /// let mut writer = Writer::from_path(&tmp_file.path()).expect("failed to create CSV writer");
    /// writer.write_record(&["header1", "header2"]).expect("failed to write header");
    /// writer.write_record(&["value1", "value2"]).expect("write record");
    /// writer.flush().expect("flush writer");
    ///
    /// let csv_builder = CsvBuilder::from_csv(tmp_file.path().to_str().unwrap());
    ///
    /// // Use get_headers and get_data to access headers and data (if present)
    /// assert!(csv_builder.get_headers().is_some());
    /// let headers = csv_builder.get_headers().unwrap();
    /// assert_eq!(headers, &["header1".to_string(), "header2".to_string()]);
    ///
    /// assert!(csv_builder.get_data().is_some());
    /// let data = csv_builder.get_data().unwrap();
    /// assert_eq!(data, &vec![vec!["value1".to_string(), "value2".to_string()]]);
    ///
    /// // Clean up temporary file (optional in doc test)
    /// tmp_file.close().expect("failed to close temporary file");
    /// ```
    ///
    /// ## Non-existent file
    ///
    /// If the specified file path doesn't point to an existing file, the `get_headers`
    /// and `get_data` methods will return `None`.
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let non_existent_path = "nonexistent_file.csv";
    ///
    /// let csv_builder = CsvBuilder::from_csv(non_existent_path);
    ///
    /// assert!(csv_builder.get_headers().is_none());
    /// assert!(csv_builder.get_data().is_none());
    /// ```
    ///
    /// ## Invalid CSV format
    ///
    /// If the file format is invalid (e.g., contains non-CSV data), the `get_headers`
    /// and `get_data` methods might return `None` (depending on the parsing behavior)
    /// and the `error` field will be set with an appropriate error.
    ///
    /// ```
    ///
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::io::Write;
    /// use csv::Writer;
    ///
    /// let mut tmp_file = tempfile::Builder::new()
    ///     .prefix("csv_test")
    ///     .suffix(".csv")
    ///     .tempfile()
    ///     .expect("failed to create temporary file");
    ///
    /// let mut writer = Writer::from_path(&tmp_file.path()).expect("failed to create CSV writer");
    /// writer.write_record(&["header1", "header2"]).expect("write header");
    /// writer.write_record(&["value1", "invalid_value"]).expect("write record (invalid value)");
    /// writer.flush().expect("flush writer");
    ///
    /// let csv_builder = CsvBuilder::from_csv(tmp_file.path().to_str().unwrap());
    ///
    /// // get_headers and get_data behavior may vary with invalid CSV
    /// assert!(csv_builder.get_headers().is_none() || csv_builder.get_headers().is_some()); // May or may not be empty
    /// assert!(csv_builder.get_data().is_none() || csv_builder.get_data().is_some());   // May or may not be empty
    ///
    /// // Clean up temporary

    pub fn from_csv(file_path: &str) -> Self {
        let mut builder = CsvBuilder::new();

        match File::open(file_path) {
            Ok(file) => {
                let mut rdr = csv::Reader::from_reader(file);

                if let Ok(hdrs) = rdr.headers() {
                    builder.headers = hdrs.iter().map(String::from).collect();
                }

                for result in rdr.records() {
                    match result {
                        Ok(record) => builder.data.push(record.iter().map(String::from).collect()),
                        Err(e) => {
                            builder.error = Some(Box::new(e));
                            break;
                        }
                    }
                }
            }
            Err(e) => builder.error = Some(Box::new(e)),
        }

        builder
    }

    /// Creates a copy of the `CsvBuilder`.
    ///
    /// This function creates a new `CsvBuilder` instance with a deep copy of the current
    /// builder's configuration, including headers, data, record limit (`limit`), and any
    /// error (`error`). This allows you to work with a copy without affecting the original
    /// builder.
    ///
    /// To verify the contents of the copy, you can use the `get_headers`, `get_data`,
    /// and `limit` methods (if applicable) on the returned `CsvBuilder`.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let original_headers = vec!["header1".to_string(), "header2".to_string()];
    /// let original_data = vec![vec!["value1".to_string(), "value2".to_string()]];
    ///
    /// let original_builder = CsvBuilder::from_raw_data(original_headers.clone(), original_data.clone());
    ///
    /// let copy = CsvBuilder::from_copy(&original_builder);
    ///
    /// assert_eq!(copy.get_headers().unwrap(), &original_headers);
    /// assert_eq!(copy.get_data().unwrap(), &original_data);
    /// ```

    pub fn from_copy(&self) -> Self {
        CsvBuilder {
            headers: self.headers.clone(),
            data: self.data.clone(),
            limit: self.limit,
            error: None,
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let executable_path = current_dir.join("python_executables/dask_joiner_connect.py");
    ///
    /// let executable_path_str = executable_path.to_str().unwrap();
    ///
    /// // Append the file name to the directory path
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///     let args = vec![
    ///         ("--file_a_path", csv_path_a_str),
    ///         ("--file_b_path", csv_path_b_str),
    ///         ("--join_type", "LEFT_JOIN"),
    ///         ("--file_a_ref_column", "id"),
    ///         ("--file_b_ref_column", "id")
    ///     ];
    ///
    ///     let mut builder = CsvBuilder::from_bare_metal_python_executable(
    ///         &executable_path_str,
    ///         args,
    ///     ).await;
    ///
    ///     let result_str = builder.print_table("75").await;
    ///     assert_eq!(builder.has_data(), true);
    /// });
    /// ```

    pub async fn from_bare_metal_python_executable(
        script_path: &str,
        args: Vec<(&str, &str)>,
    ) -> Self {
        let uid = format!(
            "{}-{}",
            Uuid::new_v4().to_string(),
            Utc::now().timestamp_millis()
        );
        let temp_filename = format!("rgwml_{}.json", &uid);

        let mut args_flat = vec![format!("--uid {}", uid)];
        for (flag, value) in &args {
            args_flat.push(format!("{} {}", flag, value));
        }

        let command_str = format!("python3 {} {}", script_path, args_flat.join(" "));

        dbg!(&command_str);
        let output = Command::new("sh").arg("-c").arg(&command_str).output();

        match output {
            Ok(output) => {
                if !output.status.success() {
                    let stderr = std::str::from_utf8(&output.stderr).unwrap_or("");
                    eprintln!("Script error: {}", stderr);
                    return CsvBuilder {
                        data: vec![],
                        headers: vec![],
                        limit: None,
                        error: Some(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            stderr,
                        ))),
                    };
                }

                let file = File::open(&temp_filename).unwrap();
                let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
                let output_str = std::str::from_utf8(&mmap).unwrap();
                let json: Value = serde_json::from_str(output_str).unwrap_or_else(|e| {
                    panic!("Failed to parse JSON: {}", e);
                });

                let headers = json["headers"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|v| v.as_str().unwrap_or("").to_string())
                    .collect();

                let rows = json["rows"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|row| {
                        row.as_array()
                            .unwrap_or(&vec![])
                            .iter()
                            .map(|cell| cell.as_str().unwrap_or("").to_string())
                            .collect()
                    })
                    .collect();

                remove_file(temp_filename).unwrap();

                CsvBuilder {
                    headers,
                    data: rows,
                    limit: None,
                    error: None,
                }
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                CsvBuilder {
                    data: vec![],
                    headers: vec![],
                    limit: None,
                    error: Some(Box::new(e)),
                }
            }
        }
    }

    /// Overrides the current `CsvBuilder` configuration with another `CsvBuilder`.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let mut builder1 = CsvBuilder::from_raw_data(
    ///     vec!["header1".to_string(), "header2".to_string()],
    ///     vec![vec!["value1".to_string(), "value2".to_string()]],
    /// );
    ///
    /// let builder2 = CsvBuilder::from_raw_data(
    ///     vec!["header3".to_string(), "header4".to_string()],
    ///     vec![vec!["value3".to_string(), "value4".to_string()]],
    /// );
    ///
    /// builder1.override_with(&builder2);
    ///
    /// // Verify builder1 is overridden
    /// assert_eq!(builder1.get_headers().unwrap(), &["header3".to_string(), "header4".to_string()]);
    /// assert_eq!(builder1.get_data().unwrap(), &vec![vec!["value3".to_string(), "value4".to_string()]]);
    /// ```
    pub fn override_with(&mut self, other: &CsvBuilder) -> &mut Self {
        self.headers = other.headers.clone();
        self.data = other.data.clone();
        self.limit = other.limit;
        self.error = None; // Uncomment this line if resetting the error state is desired.

        self
    }

    /// Instantiates a CsvBuilder object from a publicly viewable google sheet
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let csv_builder = CsvBuilder::from_publicly_viewable_google_sheet("https://docs.google.com/spreadsheets/d/1U9ozNFwV__c15z4Mp_EWorGwOv6mZPaQ9dmYtjmCPow/edit#gid=272498272").await;
    ///
    /// dbg!(&csv_builder);
    ///
    /// // Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec!["id".to_string(), "name".to_string(), "cost".to_string()];
    /// assert_eq!(csv_builder.get_headers().unwrap(), &expected_headers, "Headers do not match expected values.");
    ///
    /// let expected_data = vec![
    ///     vec!["1".to_string(), "shoes".to_string(), "900".to_string()],
    ///     vec!["2".to_string(), "slippers".to_string(), "500".to_string()],
    ///     vec!["3".to_string(), "burger".to_string(), "300".to_string()],
    ///     vec!["4".to_string(), "book".to_string(), "1000".to_string()],
    /// ];
    ///
    /// for (expected, actual) in expected_data.iter().zip(csv_builder.get_data().unwrap()) {
    ///     assert_eq!(expected[0], actual[0], "Row1 does not match expected values");
    ///     assert_eq!(expected[1], actual[1], "Row2 does not match expected values");
    ///     assert_eq!(expected[2], actual[2], "Row3 does not match expected values");
    /// }
    /// });
    /// ```
    pub async fn from_publicly_viewable_google_sheet(url: &str) -> Self {
        let mut builder = CsvBuilder::new();

        let public_url_connect_config = PublicUrlConnectConfig {
            url: url.to_string(),
            url_type: "GOOGLE_SHEETS".to_string(),
        };

        match PublicUrlConnect::get_google_sheets_data(public_url_connect_config).await {
            Ok((headers, rows)) => {
                builder.headers = headers;
                builder.data = rows;
            }
            Err(e) => {
                builder.error = Some(e);
            }
        }

        builder
    }

    /// Reads data from a specified sheet of an XLS file at the specified `file_path` and returns a `CsvBuilder`.
    ///
    /// ## Valid XLS file using simple_excel_writer
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::env;
    /// use std::path::PathBuf;
    ///
    /// // Find the relative path to the test file within the project
    /// fn locate_test_file(file_name: &str) -> PathBuf {
    ///     let mut path = env::current_dir().unwrap();
    ///     path.push("test_file_samples");
    ///     path.push(file_name);
    ///     path
    /// }
    ///
    /// let test_file_path = locate_test_file("file_example_XLS_10.xls");
    /// let csv_builder = CsvBuilder::from_xls(test_file_path.to_str().unwrap(), "Sheet1", "SHEET_NAME");
    ///
    /// // Check the headers and data
    /// assert!(csv_builder.get_headers().is_some());
    /// let headers = csv_builder.get_headers().unwrap();
    /// assert_eq!(headers, &["id".to_string(), "first_name".to_string(), "last_name".to_string(), "gender".to_string()]);
    ///
    /// assert!(csv_builder.get_data().is_some());
    /// let data = csv_builder.get_data().unwrap();
    /// assert_eq!(data, &vec![vec!["1".to_string(), "Dulce".to_string(), "Abril".to_string(), "Female".to_string()]]);
    ///
    /// let csv_builder_2 = CsvBuilder::from_xls(test_file_path.to_str().unwrap(), "1", "SHEET_ID");
    ///
    /// // Check the headers and data
    /// assert!(csv_builder_2.get_headers().is_some());
    /// let headers_2 = csv_builder_2.get_headers().unwrap();
    /// assert_eq!(headers_2, &["id".to_string(), "first_name".to_string(), "last_name".to_string(), "gender".to_string()]);
    ///
    /// assert!(csv_builder_2.get_data().is_some());
    /// let data = csv_builder_2.get_data().unwrap();
    /// assert_eq!(data, &vec![vec!["1".to_string(), "Dulce".to_string(), "Abril".to_string(), "Female".to_string()]]);
    /// ```
    /// ## Non-existent file or invalid sheet identifier
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let non_existent_path = "nonexistent_file.xls";
    ///
    /// // Test with non-existent file
    /// let csv_builder_nonexistent = CsvBuilder::from_xls(non_existent_path, "1", "SHEET_ID");
    /// assert!(csv_builder_nonexistent.get_headers().is_none());
    /// assert!(csv_builder_nonexistent.get_data().is_none());
    ///
    /// // Test with an invalid sheet identifier
    /// let invalid_sheet_path = "path_to_existing_test_file.xls";
    /// let csv_builder_invalid = CsvBuilder::from_xls(invalid_sheet_path, "99", "SHEET_ID"); // Assuming this file has fewer sheets
    /// assert!(csv_builder_invalid.get_headers().is_none());
    /// assert!(csv_builder_invalid.get_data().is_none());
    /// ```

    pub fn from_xls(file_path: &str, sheet_identifier: &str, identifier_type: &str) -> Self {
        let mut builder = CsvBuilder::new();

        match open_workbook::<Xls<_>, _>(file_path) {
            Ok(mut workbook) => {
                let sheet_names = workbook.sheet_names();
                let sheet_name_opt = match identifier_type {
                    "SHEET_NAME" => Some(sheet_identifier.to_string()),
                    "SHEET_ID" => {
                        if let Ok(index) = sheet_identifier.parse::<usize>() {
                            if index > 0 && index <= sheet_names.len() {
                                Some(sheet_names[index - 1].clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                match sheet_name_opt {
                    Some(sheet_name) => match workbook.worksheet_range(&sheet_name) {
                        Ok(range) => {
                            for row in range.rows() {
                                let row_data: Vec<String> =
                                    row.iter().map(|cell| cell.to_string()).collect();
                                if builder.headers.is_empty() {
                                    builder.headers = row_data;
                                } else {
                                    builder.data.push(row_data);
                                }
                            }
                        }
                        Err(e) => {
                            let error = Box::new(e) as Box<dyn Error>;
                            builder.error = Some(error);
                        }
                    },
                    None => {
                        let error =
                            IoError::new(ErrorKind::InvalidInput, "Sheet identifier not found");
                        builder.error = Some(Box::new(error) as Box<dyn Error>);
                    }
                }
            }
            Err(e) => {
                let error = Box::new(e) as Box<dyn Error>;
                builder.error = Some(error);
            }
        }

        builder
    }

    /// Reads data from a specified sheet of an XLSX file at the specified `file_path` and returns a `CsvBuilder`.
    ///
    /// ## Valid XLSX file
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::env;
    /// use std::path::PathBuf;
    ///
    /// // Find the relative path to the test file within the project
    /// fn locate_test_file(file_name: &str) -> PathBuf {
    ///     let mut path = env::current_dir().unwrap();
    ///     path.push("test_file_samples");
    ///     path.push(file_name);
    ///     path
    /// }
    ///
    /// let test_file_path = locate_test_file("file_example_XLS_10.xlsx");
    /// let csv_builder = CsvBuilder::from_xlsx(test_file_path.to_str().unwrap(), "Sheet1", "SHEET_NAME");
    ///
    /// // Check the headers and data
    /// assert!(csv_builder.get_headers().is_some());
    /// let headers = csv_builder.get_headers().unwrap();
    /// assert_eq!(headers, &["id".to_string(), "first_name".to_string(), "last_name".to_string(), "gender".to_string()]);
    ///
    /// assert!(csv_builder.get_data().is_some());
    /// let data = csv_builder.get_data().unwrap();
    /// assert_eq!(data, &vec![vec!["1".to_string(), "Dulce".to_string(), "Abril".to_string(), "Female".to_string()]]);
    ///
    /// let csv_builder_2 = CsvBuilder::from_xlsx(test_file_path.to_str().unwrap(), "1", "SHEET_ID");
    ///
    /// // Check the headers and data
    /// assert!(csv_builder_2.get_headers().is_some());
    /// let headers_2 = csv_builder_2.get_headers().unwrap();
    /// assert_eq!(headers_2, &["id".to_string(), "first_name".to_string(), "last_name".to_string(), "gender".to_string()]);
    ///
    /// assert!(csv_builder_2.get_data().is_some());
    /// let data = csv_builder_2.get_data().unwrap();
    /// assert_eq!(data, &vec![vec!["1".to_string(), "Dulce".to_string(), "Abril".to_string(), "Female".to_string()]]);
    /// ```
    ///
    /// ## Non-existent file or invalid sheet identifier
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let non_existent_path = "nonexistent_file.xlsx";
    ///
    /// // Test with non-existent file
    /// let csv_builder_nonexistent = CsvBuilder::from_xlsx(non_existent_path, "1", "SHEET_ID");
    /// assert!(csv_builder_nonexistent.get_headers().is_none());
    /// assert!(csv_builder_nonexistent.get_data().is_none());
    ///
    /// // Test with an invalid sheet identifier
    /// let invalid_sheet_path = "path/to/existing_test_file.xlsx";
    /// let csv_builder_invalid = CsvBuilder::from_xlsx(invalid_sheet_path, "99", "SHEET_ID");
    /// assert!(csv_builder_invalid.get_headers().is_none());
    /// assert!(csv_builder_invalid.get_data().is_none());
    /// ```

    pub fn from_xlsx(file_path: &str, sheet_identifier: &str, identifier_type: &str) -> Self {
        let mut builder = CsvBuilder::new();

        match open_workbook::<Xlsx<_>, _>(file_path) {
            Ok(mut workbook) => {
                let sheet_names = workbook.sheet_names();
                let sheet_name_opt = match identifier_type {
                    "SHEET_NAME" => Some(sheet_identifier.to_string()),
                    "SHEET_ID" => {
                        if let Ok(index) = sheet_identifier.parse::<usize>() {
                            if index > 0 && index <= sheet_names.len() {
                                Some(sheet_names[index - 1].clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                match sheet_name_opt {
                    Some(sheet_name) => match workbook.worksheet_range(&sheet_name) {
                        Ok(range) => {
                            for row in range.rows() {
                                let row_data: Vec<String> =
                                    row.iter().map(|cell| cell.to_string()).collect();
                                if builder.headers.is_empty() {
                                    builder.headers = row_data;
                                } else {
                                    builder.data.push(row_data);
                                }
                            }
                        }
                        Err(e) => {
                            let error = Box::new(e) as Box<dyn Error>;
                            builder.error = Some(error);
                        }
                    },
                    None => {
                        let error =
                            IoError::new(ErrorKind::InvalidInput, "Sheet identifier not found");
                        builder.error = Some(Box::new(error) as Box<dyn Error>);
                    }
                }
            }
            Err(e) => {
                let error = Box::new(e) as Box<dyn Error>;
                builder.error = Some(error);
            }
        }

        builder
    }

    /// Reads data from a specified dataset within an HDF5 file at the specified `file_path` and returns a `CsvBuilder`.

    ///
    /// If the dataset identifier type is `DATASET_NAME`, it accesses the dataset by its name.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dc_utils::{DcConnectConfig, DataContainer};
    /// use tokio::runtime::Runtime;
    /// use std::env::current_dir;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///     // Append the file name to the directory path
    ///     let h5_path = current_dir.join("test_file_samples/h5_test_files/sample.h5");
    ///
    ///     // Convert the path to a string
    ///     let h5_path_str = h5_path.to_str().unwrap();
    ///
    ///     let result = CsvBuilder::from_h5(h5_path_str, "0", "DATASET_ID").await;
    ///     dbg!(&result);  // Use a reference here
    ///
    ///     // Use the `get_headers` getter to check headers, handle `Option` returned
    ///     let expected_headers = vec!["Column 0".to_string(), "Column 1".to_string(), "Column 2".to_string()];
    ///     assert_eq!(result.get_headers().unwrap(), &expected_headers, "Headers do not match expected values.");
    ///
    ///     let expected_data = vec![
    ///         vec!["1.0".to_string(), "2.0".to_string(), "3.0".to_string()],
    ///         vec!["4.0".to_string(), "5.0".to_string(), "6.0".to_string()],
    ///         vec!["7.0".to_string(), "8.0".to_string(), "9.0".to_string()],
    ///     ];
    ///
    ///     for (expected, actual) in expected_data.iter().zip(result.get_data().unwrap()) {
    ///         assert_eq!(expected[0], actual[0], "Row1 does not match expected values");
    ///         assert_eq!(expected[1], actual[1], "Row2 does not match expected values");
    ///         assert_eq!(expected[2], actual[2], "Row3 does not match expected values");
    ///     }
    /// });
    /// ```
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dc_utils::{DcConnectConfig, DataContainer};
    /// use tokio::runtime::Runtime;
    /// use std::env::current_dir;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///     // Append the file name to the directory path
    ///     let h5_path = current_dir.join("test_file_samples/h5_test_files/sample_pandas.h5");
    ///
    ///     // Convert the path to a string
    ///     let h5_path_str = h5_path.to_str().unwrap();
    ///
    ///     let result = CsvBuilder::from_h5(h5_path_str, "0", "DATASET_ID").await;
    ///     dbg!(&result);  // Use a reference here
    ///
    ///     // Use the `get_headers` getter to check headers, handle `Option` returned
    ///     let expected_headers = vec!["Column1".to_string(), "Column2".to_string(), "Column3".to_string()];
    ///     assert_eq!(result.get_headers().unwrap(), &expected_headers, "Headers do not match expected values.");
    ///
    ///     let expected_data = vec![
    ///         vec!["1".to_string(), "A".to_string(), "True".to_string()],
    ///         vec!["2".to_string(), "B".to_string(), "False".to_string()],
    ///         vec!["3".to_string(), "C".to_string(), "True".to_string()],
    ///         vec!["4".to_string(), "D".to_string(), "False".to_string()],
    ///     ];
    ///
    ///     for (expected, actual) in expected_data.iter().zip(result.get_data().unwrap()) {
    ///         assert_eq!(expected[0], actual[0], "Row1 does not match expected values");
    ///         assert_eq!(expected[1], actual[1], "Row2 does not match expected values");
    ///         assert_eq!(expected[2], actual[2], "Row3 does not match expected values");
    ///     }
    /// });
    /// ```

    pub async fn from_h5(file_path: &str, dataset_identifier: &str, identifier_type: &str) -> Self {
        let mut builder = CsvBuilder::new();

        let dc_connect_config = DcConnectConfig {
            path: file_path.to_string(),
            dc_type: "H5".to_string(),
            h5_dataset_identifier: dataset_identifier.to_string(),
            h5_identifier_type: identifier_type.to_string(),
        };

        match DataContainer::get_dc_data(dc_connect_config).await {
            Ok((headers, rows)) => {
                builder.headers = headers;
                builder.data = rows;
            }
            Err(e) => {
                builder.error = Some(e);
            }
        }

        builder
    }

    /// Creates a `CsvBuilder` instance from headers and data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    /// ];
    ///
    /// let csv_builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// // Check that the headers are correct
    /// assert!(csv_builder.get_headers().is_some());
    /// let retrieved_headers = csv_builder.get_headers().unwrap();
    /// assert_eq!(retrieved_headers, &headers);
    ///
    /// // Check that the data is correct
    /// assert!(csv_builder.get_data().is_some());
    /// let retrieved_data = csv_builder.get_data().unwrap();
    /// assert_eq!(retrieved_data, &data);
    /// ```
    pub fn from_raw_data(headers: Vec<String>, data: Vec<Vec<String>>) -> Self {
        CsvBuilder {
            headers,
            data,
            limit: None,
            error: None,
        }
    }

    /// Creates a `CsvBuilder` instance directly from an MSSQL query.
    pub async fn from_mssql_query(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let result =
            DbConnect::execute_mssql_query(username, password, server, database, sql_query).await?;

        Ok(CsvBuilder::from_raw_data(result.0, result.1))
    }

    /// Retrieves column descriptions from the specified table within an MSSQL database.
    pub async fn get_mssql_table_description(
        username: &str,
        password: &str,
        server: &str,
        in_focus_database: &str,
        table_name: &str,
    ) -> Result<CsvBuilder, Box<dyn std::error::Error>> {
        // SQL query to fetch column details similar to 'sp_help'
        let column_query = format!(
            "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLLATION_NAME 
        FROM {}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{}'",
            in_focus_database, table_name
        );

        //dbg!(&column_query);
        let result = CsvBuilder::from_mssql_query(
            username,
            password,
            server,
            in_focus_database,
            &column_query,
        )
        .await?;
        //let _ = &result.print_table_all_rows();

        Ok(result)
    }

    /// Creates a `CsvBuilder` instance directly from an MSSQL query, receiving the data in chunks.
    pub async fn from_chunked_mssql_query_union(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<CsvBuilder> = None;

        loop {
            // Construct the chunked query using OFFSET and FETCH NEXT for MSSQL
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery ORDER BY (SELECT NULL) OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                sql_query, offset, chunk_size
            );

            chunk_query.retain(|c| c != ';');
            // Execute the chunked query
            let result =
                CsvBuilder::from_mssql_query(username, password, server, database, &chunk_query)
                    .await?;

            // If the chunk has no data, break the loop
            if result.data.is_empty() {
                break;
            }

            //result.print_table();

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .union_with_csv_builder(result, "UNION", "", "", "75")
                        .await
                        .print_table("75")
                        .await;
                }
                //None => combined_builder = Some(result),
                None => {
                    combined_builder = Some(result);
                    if let Some(builder) = &mut combined_builder {
                        builder.print_table("75").await;
                    }
                }
            }

            // Update offset for the next chunk
            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `CsvBuilder` instance directly from an MSSQL query, receiving the data in chunks,
    /// as a bag union
    pub async fn from_chunked_mssql_query_bag_union(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<CsvBuilder> = None;

        loop {
            // Construct the chunked query using OFFSET and FETCH NEXT for MSSQL
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery ORDER BY (SELECT NULL) OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                sql_query, offset, chunk_size
            );

            chunk_query.retain(|c| c != ';');
            // Execute the chunked query
            let result =
                CsvBuilder::from_mssql_query(username, password, server, database, &chunk_query)
                    .await?;

            // If the chunk has no data, break the loop
            if result.data.is_empty() {
                break;
            }

            //result.print_table();

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        //.set_bag_union_with_csv_builder(&mut result)
                        .union_with_csv_builder(result, "UNION", "", "", "75")
                        .await
                        .print_table("75")
                        .await;
                }
                //None => combined_builder = Some(result),
                None => {
                    combined_builder = Some(result);
                    if let Some(builder) = &mut combined_builder {
                        builder.print_table("75").await;
                    }
                }
            }

            // Update offset for the next chunk
            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `CsvBuilder` instance directly from an MySQL query.
    pub async fn from_mysql_query(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let result =
            DbConnect::execute_mysql_query(username, password, server, database, sql_query).await?;

        Ok(CsvBuilder::from_raw_data(result.0, result.1))
    }

    /// Creates a `CsvBuilder` instance directly from a MySQL query, receiving the data in chunks.
    pub async fn from_chunked_mysql_query_union(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<CsvBuilder> = None;

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            //dbg!(&chunk_query);
            chunk_query.retain(|c| c != ';');
            let result =
                CsvBuilder::from_mysql_query(username, password, server, database, &chunk_query)
                    .await?;

            if result.data.is_empty() {
                break;
            }

            //result.print_table();

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        //.set_union_with_csv_builder(&mut result, "UNION_TYPE:NORMAL", vec!["*"])
                        .union_with_csv_builder(result, "UNION", "", "", "75")
                        .await
                        .print_table("75")
                        .await;
                }
                //None => combined_builder = Some(result),
                None => {
                    combined_builder = Some(result);
                    if let Some(builder) = &mut combined_builder {
                        builder.print_table("75").await;
                    }
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `CsvBuilder` instance directly from a MySQL query, receiving the data in chunks,
    /// collated as a bag union
    pub async fn from_chunked_mysql_query_bag_union(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<CsvBuilder> = None;

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            //dbg!(&chunk_query);
            chunk_query.retain(|c| c != ';');
            let result =
                CsvBuilder::from_mysql_query(username, password, server, database, &chunk_query)
                    .await?;

            if result.data.is_empty() {
                break;
            }

            //result.print_table();

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        // .set_bag_union_with_csv_builder(&mut result)
                        .union_with_csv_builder(result, "BAG_UNION", "", "", "75")
                        .await
                        .print_table("75")
                        .await;
                }
                //None => combined_builder = Some(result),
                None => {
                    combined_builder = Some(result);
                    if let Some(builder) = &mut combined_builder {
                        builder.print_table("75").await;
                    }
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Retrieves column descriptions from the specified table within a MySQL database.
    pub async fn get_mysql_table_description(
        username: &str,
        password: &str,
        server: &str,
        in_focus_database: &str,
        table_name: &str,
    ) -> Result<CsvBuilder, Box<dyn Error>> {
        // SQL query to fetch column details similar to 'DESCRIBE tablename' or 'SHOW COLUMNS FROM tablename'
        let column_query = format!(
            "SHOW FULL COLUMNS FROM {}.{}",
            in_focus_database, table_name
        );

        //dbg!(&column_query);
        // Use CsvBuilder's from_mysql_query method to execute the query and handle results
        let mut result = CsvBuilder::from_mysql_query(
            username,
            password,
            server,
            in_focus_database,
            &column_query,
        )
        .await?;

        let _ = result.retain_columns(vec![
            "Field",
            "Type",
            "Null",
            "Default",
            "Extra",
            "Collation",
        ]);

        Ok(result)
    }

    /// Creates a `CsvBuilder` instance directly from a ClickHouse query.
    pub async fn from_clickhouse_query(
        username: &str,
        password: &str,
        server: &str,
        sql_query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let result =
            DbConnect::execute_clickhouse_query(username, password, server, sql_query).await?;

        Ok(CsvBuilder::from_raw_data(result.0, result.1))
    }

    /// Creates a `CsvBuilder` instance directly from a ClickHouse query, receiving the data in chunks
    pub async fn from_chunked_clickhouse_query_union(
        username: &str,
        password: &str,
        server: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<CsvBuilder> = None;

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            //dbg!(&chunk_query);
            chunk_query.retain(|c| c != ';');
            let result =
                CsvBuilder::from_clickhouse_query(username, password, server, &chunk_query).await?;

            if result.data.is_empty() {
                break;
            }

            //result.print_table();

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        // .set_union_with_csv_builder(&mut result, "UNION_TYPE:NORMAL", vec!["*"])
                        .union_with_csv_builder(result, "UNION", "", "", "75")
                        .await
                        .print_table("75")
                        .await;
                }
                //None => combined_builder = Some(result),
                None => {
                    combined_builder = Some(result);
                    if let Some(builder) = &mut combined_builder {
                        builder.print_table("75").await;
                    }
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `CsvBuilder` instance directly from a ClickHouse query, receiving the data in chunks,
    /// collated as a bag union
    pub async fn from_chunked_clickhouse_query_bag_union(
        username: &str,
        password: &str,
        server: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<CsvBuilder> = None;

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            //dbg!(&chunk_query);
            chunk_query.retain(|c| c != ';');
            let result =
                CsvBuilder::from_clickhouse_query(username, password, server, &chunk_query).await?;

            if result.data.is_empty() {
                break;
            }

            //result.print_table();

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        // .set_bag_union_with_csv_builder(&mut result)
                        .union_with_csv_builder(result, "BAG_UNION", "", "", "75")
                        .await
                        .print_table("75")
                        .await;
                }
                //None => combined_builder = Some(result),
                None => {
                    combined_builder = Some(result);
                    if let Some(builder) = &mut combined_builder {
                        builder.print_table("75").await;
                    }
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Retrieves column descriptions from the specified table within a ClickHouse database.
    pub async fn get_clickhouse_table_description(
        username: &str,
        password: &str,
        server: &str,
        in_focus_database: &str,
        table_name: &str,
    ) -> Result<CsvBuilder, Box<dyn Error>> {
        // SQL query to fetch column details similar to 'DESCRIBE tablename' or 'SHOW COLUMNS FROM tablename'
        let column_query = format!(
            "SHOW FULL COLUMNS FROM {}.{}",
            in_focus_database, table_name
        );

        //dbg!(&column_query);
        // Use CsvBuilder's from_mysql_query method to execute the query and handle results
        let result =
            CsvBuilder::from_clickhouse_query(username, password, server, &column_query).await?;

        Ok(result)
    }

    /// Creates a `CsvBuilder` instance directly from a ClickHouse query.
    pub async fn from_google_big_query_query(
        json_credentials_path: &str,
        sql_query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let result =
            DbConnect::execute_google_big_query_query(json_credentials_path, sql_query).await?;

        Ok(CsvBuilder::from_raw_data(result.0, result.1))
    }

    /// Creates a `CsvBuilder` instance directly from a ClickHouse query, receiving the data in chunks
    pub async fn from_chunked_google_big_query_query_union(
        json_credentials_path: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<CsvBuilder> = None;

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            //dbg!(&chunk_query);
            chunk_query.retain(|c| c != ';');
            let result =
                CsvBuilder::from_google_big_query_query(json_credentials_path, &chunk_query)
                    .await?;

            if result.data.is_empty() {
                break;
            }

            //result.print_table();

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        //.set_union_with_csv_builder(&mut result, "UNION_TYPE:NORMAL", vec!["*"])
                        .union_with_csv_builder(result, "UNION", "", "", "75")
                        .await
                        .print_table("75")
                        .await;
                }
                //None => combined_builder = Some(result),
                None => {
                    combined_builder = Some(result);
                    if let Some(builder) = &mut combined_builder {
                        builder.print_table("75").await;
                    }
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `CsvBuilder` instance directly from a ClickHouse query, receiving the data in chunks
    pub async fn from_chunked_google_big_query_query_bag_union(
        json_credentials_path: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<CsvBuilder> = None;

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            //dbg!(&chunk_query);
            chunk_query.retain(|c| c != ';');
            let result =
                CsvBuilder::from_google_big_query_query(json_credentials_path, &chunk_query)
                    .await?;

            if result.data.is_empty() {
                break;
            }

            //result.print_table();

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        // .set_bag_union_with_csv_builder(&mut result)
                        .union_with_csv_builder(result, "BAG_UNION", "", "", "75")
                        .await
                        .print_table("75")
                        .await;
                }
                //None => combined_builder = Some(result),
                None => {
                    combined_builder = Some(result);
                    if let Some(builder) = &mut combined_builder {
                        builder.print_table("75").await;
                    }
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Retrieves column descriptions from the specified table within a ClickHouse database.
    pub async fn get_google_big_query_table_description(
        json_credentials_path: &str,
        project_id: &str,
        dataset: &str,
        table_name: &str,
    ) -> Result<CsvBuilder, Box<dyn Error>> {
        // SQL query to fetch column details similar to 'DESCRIBE tablename' or 'SHOW COLUMNS FROM tablename'

        //dbg!(&project_id, &dataset);
        let column_query = format!(
            "SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{}'",
            project_id, dataset, table_name
        );

        //dbg!(&column_query);
        // Use CsvBuilder's from_mysql_query method to execute the query and handle results
        let result =
            CsvBuilder::from_google_big_query_query(json_credentials_path, &column_query).await?;

        Ok(result)
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// // Append the relative path to the current directory
    /// let csv_dir = current_dir.join("test_file_samples/xgb_test_files");
    ///
    /// // Convert the path to a string
    /// let csv_path = csv_dir.to_str().unwrap();

    /// let mut csv_builder = CsvBuilder::get_all_csv_files(csv_path).expect("Failed to load CSV files");
    ///
    /// csv_builder.print_table_all_rows();
    /// // Validate headers
    /// let headers = csv_builder.get_headers().expect("CsvBuilder should contain headers");
    /// assert_eq!(headers, &["file_name".to_string(), "last_modified".to_string(), "mb_size".to_string()]);
    ///
    /// // Validate data
    /// let data = csv_builder.get_data().expect("CsvBuilder should contain data");
    /// assert!(!data.is_empty(), "CsvBuilder data should not be empty");
    /// // Additional assertions can be added based on expected data
    /// ```

    pub fn get_all_csv_files(path: &str) -> Result<CsvBuilder, Box<dyn Error>> {
        // Read the directory
        let dir_entries = read_dir(path)?;

        // Create a new CsvBuilder and set the headers
        let mut csv_builder = CsvBuilder::new();
        csv_builder.set_header(vec!["file_name", "last_modified", "mb_size"]);

        // Iterate through each file in the directory
        for entry in dir_entries {
            let entry = entry?;
            let file_path = entry.path();

            // Only process JSON files
            if file_path.extension().map_or(false, |ext| ext == "csv") {
                let file_name = file_path.file_name().unwrap().to_string_lossy().to_string();

                // Get the file metadata
                let metadata = metadata(&file_path)?;
                let modified_time = metadata.modified()?;
                let file_size = metadata.len(); // Get the file size

                // Convert the file size to megabytes
                let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

                // Calculate the duration since the UNIX epoch
                let duration_since_epoch = modified_time
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards");
                let timestamp = duration_since_epoch.as_secs();

                let timestamp_i64 = timestamp as i64;

                // Convert the timestamp to a DateTime<Utc>
                let datetime: DateTime<Utc> = Utc.timestamp_opt(timestamp_i64, 0).unwrap();

                // Format the datetime as a string
                let formatted_timestamp = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
                let formatted_file_size = format!("{:.2}", file_size_mb);
                // Add a row to the CsvBuilder
                csv_builder.add_row(vec![&file_name, &formatted_timestamp, &formatted_file_size]);
            }
        }

        Ok(csv_builder)
    }

    /// Checks if a CSV file was last modified within a given number of minutes.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    /// let csv_path = current_dir.join("test_file_samples/sales_data.csv");
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let within_time_range = CsvBuilder::was_last_modified_within(csv_path_str, "1");
    /// dbg!(&within_time_range);
    /// assert!(!within_time_range);
    /// ```
    pub fn was_last_modified_within(path: &str, minutes: &str) -> bool {
        if let Ok(metadata) = metadata(path) {
            if let Ok(modified_time) = metadata.modified() {
                if let Ok(minutes) = minutes.parse::<i64>() {
                    let duration = ChronoDuration::minutes(minutes);

                    let current_time = SystemTime::now();
                    let modified_time = DateTime::<Utc>::from(modified_time);
                    let current_time = DateTime::<Utc>::from(current_time);

                    return current_time.signed_duration_since(modified_time) <= duration;
                }
            }
        }
        false
    }

    /// Calibrates a poorly formatted Csv File
    pub fn calibrate(&mut self, config: CalibConfig) -> &mut Self {
        // Parse header_is_at_row to usize, default to 0 if parsing fails
        let header_index = config
            .header_is_at_row
            .parse::<usize>()
            .unwrap_or(0)
            .saturating_sub(2);

        // Set the header and remove the header row from the data
        if header_index < self.data.len() {
            if let Some(header_row) = self.data.get(header_index).cloned() {
                self.headers = header_row;
                self.data.remove(header_index);
            }
        }

        // Parse start index of rows_range_from to usize, default to 0 if parsing fails
        let start_index = config
            .rows_range_from
            .0
            .parse::<usize>()
            .unwrap_or(0)
            .saturating_sub(3);

        // Determine the end_index based on the second value of rows_range_from
        let end_index = match config.rows_range_from.1.as_str() {
            "*" => self.data.len(), // "*" represents 'until the end'
            end_str => end_str
                .parse::<usize>()
                .unwrap_or(self.data.len())
                .saturating_sub(2),
        };

        // Debug information
        //dbg!(&start_index, &end_index, self.data.get(start_index), self.data.get(end_index.saturating_sub(1)));

        // Filter the data based on the calculated range
        if start_index < self.data.len() {
            let end_index = std::cmp::min(end_index, self.data.len());
            self.data = self.data[start_index..end_index].to_vec();
        }

        self
    }

    /// Saves data in the `CsvBuilder` to a new CSV file at `new_file_path`.
    pub fn save_as(&mut self, new_file_path: &str) -> Result<&mut Self, Box<dyn Error>> {
        let file = File::create(new_file_path)?;
        let mut wtr = csv::Writer::from_writer(file);

        // dbg!(&self.headers);
        // dbg!(&self.data);

        // Write the headers
        if !self.headers.is_empty() {
            wtr.write_record(&self.headers)?;
        }

        // Ensure each data row has the same number of elements as there are headers
        let headers_len = self.headers.len();
        for record in &mut self.data {
            // Pad the record with empty strings if it has fewer elements than headers
            while record.len() < headers_len {
                record.push("".to_string());
            }

            // dbg!(&record, &new_file_path);
            wtr.write_record(record)?;
        }

        wtr.flush()?;

        Ok(self)
    }

    /// Groups data by the given column and value, then saves each group to a CSV file.
    pub fn split_as(&mut self, column_name: &str, folder_path: &str) -> Result<(), Box<dyn Error>> {
        let column_index = self
            .headers
            .iter()
            .position(|h| h == column_name)
            .ok_or("Column name not found")?;

        // Group data by the specified column value
        let mut groups: HashMap<String, Vec<Vec<String>>> = HashMap::new();
        for row in &self.data {
            if let Some(value) = row.get(column_index) {
                groups
                    .entry(value.clone())
                    .or_insert_with(Vec::new)
                    .push(row.clone());
            }
        }

        // Create a CSV file for each group
        for (value, rows) in groups {
            let file_name = format!(
                "{}/group_split_by_{}_in_{}.csv",
                folder_path, value, column_name
            );
            let file = File::create(file_name)?;
            let mut wtr = Writer::from_writer(file);

            // Write the headers
            wtr.write_record(&self.headers)?;

            // Write the data rows for this group
            for row in rows {
                wtr.write_record(&row)?;
            }

            wtr.flush()?;
        }

        Ok(())
    }

    /// Transposes a CsvBuilder object
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["product".to_string(), "sales_q1".to_string(), "sales_q2".to_string(), "sales_q3".to_string(), "sales_q4".to_string()];
    /// let data = vec![
    ///     vec!["A".to_string(), "100".to_string(), "150".to_string(), "200".to_string(), "250".to_string()],
    ///     vec!["B".to_string(), "80".to_string(), "120".to_string(), "160".to_string(), "200".to_string()],
    ///     vec!["C".to_string(), "90".to_string(), "110".to_string(), "130".to_string(), "170".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// csv_builder.transpose_transform().print_table("75").await;
    /// dbg!(&csv_builder);
    ///
    /// // Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec!["product".to_string(), "A".to_string(), "B".to_string(), "C".to_string()];
    /// assert_eq!(csv_builder.get_headers().unwrap(), &expected_headers, "Headers do not match expected values.");
    ///
    /// let expected_data = vec![
    ///     vec!["sales_q1".to_string(), "100".to_string(), "80".to_string(), "90".to_string()],
    ///     vec!["sales_q2".to_string(), "150".to_string(), "120".to_string(), "110".to_string()],
    ///     vec!["sales_q3".to_string(), "200".to_string(), "160".to_string(), "130".to_string()],
    ///     vec!["sales_q4".to_string(), "250".to_string(), "200".to_string(), "170".to_string()],
    /// ];
    ///
    /// for (expected, actual) in expected_data.iter().zip(csv_builder.get_data().unwrap()) {
    ///     assert_eq!(expected[0], actual[0], "Row1 does not match expected values");
    ///     assert_eq!(expected[1], actual[1], "Row2 does not match expected values");
    ///     assert_eq!(expected[2], actual[2], "Row3 does not match expected values");
    ///     assert_eq!(expected[3], actual[3], "Row4 does not match expected values");
    /// }
    /// });
    /// ```
    pub fn transpose_transform(&mut self) -> &mut Self {
        if self.data.is_empty() {
            self.error = Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Data is empty",
            )));
            return self;
        }

        // Create new headers from the first column of data
        let mut new_headers = vec![self.headers[0].clone()];
        new_headers.extend(self.data.iter().map(|row| row[0].clone()));

        // Transpose the data, excluding the first column
        let transposed_data: Vec<Vec<String>> = (1..self.headers.len())
            .into_par_iter()
            .map(|i| {
                let mut new_row = Vec::with_capacity(self.data.len() + 1);
                new_row.push(self.headers[i].clone());
                new_row.extend(self.data.iter().map(|row| row[i].clone()));
                new_row
            })
            .collect();

        // Update headers and data in the object
        self.headers = new_headers;
        self.data = transposed_data;

        self
    }

    /// Groups data by a specified column and transforms the grouped data into a new column containing serialized JSON strings, such that the result is sorted as per the specified column in ascending order, and the elements of grouped data are arranged in a consistent order of key value pairs as per the original builder object.
    ///
    /// This method restructures the CSV data by grouping rows based on the values of a specified column
    /// and then serializing the grouped rows into JSON format. It replaces the current headers with the
    /// group column, a new column for the transformed data, and additional columns based on specified feature flags.
    /// This is particularly useful for aggregation or detailed analysis of subsets within the data.
    ///
    /// # Arguments
    ///
    /// * `group_by` - A string slice that specifies the header name of the column to group the data by.
    /// * `new_column_name` - A string slice that specifies the name of the new column where the grouped
    ///                       and transformed data will be stored.
    /// * `feature_flags` - A vector of tuples where each tuple contains a column name and a feature flag
    ///                     that specifies the type of aggregation or calculation to be applied to the column.
    ///
    /// # Feature Flags
    ///
    /// The following feature flags can be used to perform different types of calculations on the specified columns:
    /// - `COUNT_UNIQUE` - Counts the unique values in the column.
    /// - `NUMERICAL_MAX` - Finds the maximum numerical value in the column.
    /// - `NUMERICAL_MIN` - Finds the minimum numerical value in the column.
    /// - `NUMERICAL_SUM` - Calculates the sum of numerical values in the column.
    /// - `NUMERICAL_MEAN` - Calculates the mean (average) of numerical values in the column, rounded to two decimal places.
    /// - `NUMERICAL_MEDIAN` - Calculates the median of numerical values in the column, rounded to two decimal places.
    /// - `NUMERICAL_STANDARD_DEVIATION` - Calculates the standard deviation of numerical values in the column, rounded to two decimal places.
    /// - `DATETIME_MAX` - Finds the maximum datetime value in the column, based on specified formats.
    /// - `DATETIME_MIN` - Finds the minimum datetime value in the column, based on specified formats.
    /// - `DATETIME_COMMA_SEPARATED` - Comma separates datetime values in the columns
    /// - `MODE` - Finds the most frequent value in the column.
    /// - `BOOL_PERCENT` - Calculates the percentage of `1`s in the column, assuming the values are either `1` or `0`, rounded to two decimal places.
    ///
    /// # Panics
    ///
    /// - Panics if the specified `group_by` column does not exist in the headers.
    /// - Panics if any row does not contain enough columns as expected based on the headers, indicating
    ///   an index out of bounds.
    ///
    ///
    /// # Examples

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskGrouperConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "flag".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "1".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "0".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "1".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "28".to_string(), "2023-01-01 11:00:00".to_string(), "0".to_string()]
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let dask_grouper_config = DaskGrouperConfig {
    ///     group_by_column_name: "category".to_string(),
    ///     count_unique_agg_columns: "name".to_string(),
    ///     numerical_max_agg_columns: "age".to_string(),
    ///     numerical_min_agg_columns: "age".to_string(),
    ///     numerical_sum_agg_columns: "age".to_string(),
    ///     numerical_mean_agg_columns: "age".to_string(),
    ///     numerical_median_agg_columns: "age".to_string(),
    ///     numerical_std_deviation_agg_columns: "age".to_string(),
    ///     mode_agg_columns: "name".to_string(),
    ///     datetime_max_agg_columns: "date".to_string(),
    ///     datetime_min_agg_columns: "date".to_string(),
    ///     datetime_semi_colon_separated_agg_columns: "date".to_string(),
    ///     bool_percent_agg_columns: "flag".to_string(),
    /// };
    ///
    /// csv_builder.grouped_index_transform(
    ///     dask_grouper_config
    /// ).await;
    ///
    ///
    /// csv_builder.print_table("75").await;
    /// dbg!(&csv_builder);
    ///
    /// // Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec![
    ///     "category".to_string(),
    ///     "COUNT_TOTAL".to_string(),
    ///     "COUNT_UNIQUE".to_string(),
    ///     "age_NUMERICAL_MAX".to_string(),
    ///     "age_NUMERICAL_MEAN".to_string(),
    ///     "age_NUMERICAL_MEDIAN".to_string(),
    ///     "age_NUMERICAL_MIN".to_string(),
    ///     "age_NUMERICAL_STANDARD_DEVIATION".to_string(),
    ///     "age_NUMERICAL_SUM".to_string(),
    ///     "date_DATETIME_MAX".to_string(),
    ///     "date_DATETIME_MIN".to_string(),
    ///     "date_DATETIME_SEMI_COLON_SEPARATED".to_string(),
    ///     "flag_BOOL_PERCENT".to_string(),
    ///     "name_COUNT_UNIQUE".to_string(),
    ///     "name_MODE".to_string()
    ///     ];
    /// assert_eq!(csv_builder.get_headers().unwrap(), &expected_headers, "Headers do not match expected values.");
    ///
    ///
    ///
    /// let expected_data = vec![
    ///     vec!["1".to_string(), "3".to_string(), "3".to_string(), "30.0".to_string(), "27.67".to_string(), "28.0".to_string(), "25.0".to_string(), "2.52".to_string(), "83.0".to_string(), "2023-01-02 13:00:00".to_string(), "2023-01-01 11:00:00".to_string(), "2023-01-01 12:00:00;2023-01-02 13:00:00;2023-01-01 11:00:00".to_string(), "66.67".to_string(), "2".to_string(), "Charlie".to_string()],
    ///     vec!["2".to_string(), "1".to_string(), "1".to_string(), "22.0".to_string(), "22.0".to_string(), "22.0".to_string(),  "22.0".to_string(), "nan".to_string(), "22.0".to_string(), "2022-12-31 11:59:59".to_string(), "2022-12-31 11:59:59".to_string(), "2022-12-31 11:59:59".to_string(), "0.0".to_string(), "1".to_string(), "Bob".to_string()],
    /// ];
    ///
    /// for (expected, actual) in expected_data.iter().zip(csv_builder.get_data().unwrap()) {
    ///     assert_eq!(expected[0], actual[0], "Count does not match expected values");
    ///     assert_eq!(expected[1], actual[1], "Count does not match expected values");
    ///     assert_eq!(expected[2], actual[2], "Count does not match expected values");
    ///     assert_eq!(expected[3], actual[3], "Unique count does not match expected values");
    ///     assert_eq!(expected[4], actual[4], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[5], actual[5], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[6], actual[6], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[7], actual[7], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[8], actual[8], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[9], actual[9], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[10], actual[10], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[11], actual[11], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[12], actual[12], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[13], actual[13], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[14], actual[14], "Feature flag values do not match expected values");
    /// }
    /// });
    /// ```

    ///
    /// Example 2: Testing the ability of the COUNT_UNIQUE column to aggregate extent of row
    /// duplication
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskGrouperConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "flag".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "1".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "0".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "1".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "28".to_string(), "2023-01-01 11:00:00".to_string(), "0".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "28".to_string(), "2023-01-01 11:00:00".to_string(), "0".to_string()]
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let dask_grouper_config = DaskGrouperConfig {
    ///     group_by_column_name: "category".to_string(),
    ///     count_unique_agg_columns: "".to_string(),
    ///     numerical_max_agg_columns: "".to_string(),
    ///     numerical_min_agg_columns: "".to_string(),
    ///     numerical_sum_agg_columns: "".to_string(),
    ///     numerical_mean_agg_columns: "".to_string(),
    ///     numerical_median_agg_columns: "".to_string(),
    ///     numerical_std_deviation_agg_columns: "".to_string(),
    ///     mode_agg_columns: "".to_string(),
    ///     datetime_max_agg_columns: "".to_string(),
    ///     datetime_min_agg_columns: "".to_string(),
    ///     datetime_semi_colon_separated_agg_columns: "".to_string(),
    ///     bool_percent_agg_columns: "".to_string(),
    /// };
    ///
    /// csv_builder.grouped_index_transform(
    ///     dask_grouper_config
    /// ).await;
    ///
    ///
    /// csv_builder.print_table("75").await;
    /// dbg!(&csv_builder);
    ///
    /// // Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec![
    ///     "category".to_string(),
    ///     "COUNT_TOTAL".to_string(),
    ///     "COUNT_UNIQUE".to_string(),
    ///     ];
    /// assert_eq!(csv_builder.get_headers().unwrap(), &expected_headers, "Headers do not match expected values.");
    ///
    ///
    ///
    /// let expected_data = vec![
    ///     vec!["1".to_string(), "4".to_string(), "3".to_string()],
    ///     vec!["2".to_string(), "1".to_string(), "1".to_string()],
    /// ];
    ///
    /// for (expected, actual) in expected_data.iter().zip(csv_builder.get_data().unwrap()) {
    ///     assert_eq!(expected[0], actual[0], "Count does not match expected values");
    ///     assert_eq!(expected[1], actual[1], "Count does not match expected values");
    ///     assert_eq!(expected[2], actual[2], "Count does not match expected values");
    /// }
    /// });
    /// //assert_eq!(1,2);
    /// ```

    pub async fn grouped_index_transform(
        &mut self,
        dask_grouper_config: DaskGrouperConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            /*
            // Read and print the first 10 rows of the temporary file
            if let Ok(file) = File::open(temp_file.path()) {
                let reader = io::BufReader::new(file);
                for (index, line) in reader.lines().enumerate() {
                    if index < 10 {
                        if let Ok(line) = line {
                            println!("{}", line);
                        }
                    } else {
                        break;
                    }
                }
            } else {
                println!("Failed to open temp file for reading");
            }
            */

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_grouper(csv_path_str, dask_grouper_config).await {
                    Ok((headers, rows)) => {
                        self.headers = headers;
                        self.data = rows;
                    }
                    Err(e) => {
                        println!("{}", &e);
                        // Handle the error, possibly by logging or returning a default value
                        // For now, we just set empty headers and data
                        self.headers = vec![];
                        self.data = vec![];
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskPivoterConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let csv_path = current_dir.join("test_file_samples/pivoting_test_files/employees.csv");
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let mut csv_builder = CsvBuilder::from_csv(&csv_path_str);
    ///
    /// let dask_pivoter_config = DaskPivoterConfig {
    ///     group_by_column_name: "department".to_string(),
    ///     values_to_aggregate_column_name: "salary".to_string(),
    ///     operation: "NUMERICAL_MEAN".to_string(),
    ///     segregate_by_column_names: "is_manager, gender".to_string(),
    /// };
    ///
    /// csv_builder.pivot(
    ///     dask_pivoter_config
    /// ).await;
    ///
    ///
    /// csv_builder.print_table("75").await;
    /// dbg!(&csv_builder);
    ///
    /// // Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec![
    ///     "department".to_string(),
    ///     "[is_manager(0)]&&[gender(F)]".to_string(),
    ///     "[is_manager(0)]&&[gender(M)]".to_string(),
    ///     "[is_manager(1)]&&[gender(F)]".to_string(),
    ///     "[is_manager(1)]&&[gender(M)]".to_string(),
    ///     "OVERALL_NUMERICAL_MEAN(salary)".to_string(),
    ///     ];
    /// assert_eq!(csv_builder.get_headers().unwrap(), &expected_headers, "Headers do not match expected values.");
    ///
    ///
    /// let expected_data = vec![
    ///     vec!["Engineering".to_string(), "98000.0".to_string(), "0.0".to_string(), "0.0".to_string(), "100000.0".to_string(), "198000.0".to_string()],
    ///     vec!["HR".to_string(), "90000.0".to_string(), "0.0".to_string(), "95000.0".to_string(), "75000.0".to_string(), "260000.0".to_string()],
    ///     vec!["Sales".to_string(), "0.0".to_string(), "52500.0".to_string(), "0.0".to_string(), "105000.0".to_string(), "157500.0".to_string()],
    /// ];
    ///
    /// for (expected, actual) in expected_data.iter().zip(csv_builder.get_data().unwrap()) {
    ///     assert_eq!(expected[0], actual[0], "Count does not match expected values");
    ///     assert_eq!(expected[1], actual[1], "Count does not match expected values");
    ///     assert_eq!(expected[2], actual[2], "Count does not match expected values");
    ///     assert_eq!(expected[3], actual[3], "Unique count does not match expected values");
    ///     assert_eq!(expected[4], actual[4], "Feature flag values do not match expected values");
    ///     assert_eq!(expected[5], actual[5], "Feature flag values do not match expected values");
    /// }
    /// });
    /// ```
    pub async fn pivot(&mut self, dask_pivoter_config: DaskPivoterConfig) -> &mut Self {
        //dbg!("BBBB");

        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            /*
            // Write data to the temporary file
            for row in &temp_builder.data {
                let _ = writeln!(temp_file, "{}", row.join(","));
            }
            */

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            /*
            // Read and print the first 10 rows of the temporary file
            if let Ok(file) = File::open(temp_file.path()) {
                let reader = io::BufReader::new(file);
                for (index, line) in reader.lines().enumerate() {
                    if index < 10 {
                        if let Ok(line) = line {
                            println!("{}", line);
                        }
                    } else {
                        break;
                    }
                }
            } else {
                println!("Failed to open temp file for reading");
            }
            */

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                //dbg!(csv_path_str);
                match DaskConnect::dask_pivoter(csv_path_str, dask_pivoter_config).await {
                    Ok((headers, rows)) => {
                        //dbg!(&headers);

                        //dbg!(&rows);
                        self.headers = headers;
                        self.data = rows;
                    }
                    Err(e) => {
                        println!("{}", &e);
                        // Handle the error, possibly by logging or returning a default value
                        // For now, we just set empty headers and data
                        self.headers = vec![];
                        self.data = vec![];
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskCleanerConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let mut csv_builder = CsvBuilder::from_csv(&csv_path_str);
    ///
    /// let dask_cleaner_config = DaskCleanerConfig {
    ///     rules: "mobile:IS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER".to_string(),
    ///     action: "ANALYZE_AND_CLEAN".to_string(),
    ///     show_unclean_values_in_report: "TRUE".to_string(),
    /// };
    ///
    /// csv_builder.clean_or_test_clean_by_eliminating_rows_subject_to_column_parse_rules(
    ///     dask_cleaner_config
    /// ).await;
    ///
    ///
    /// csv_builder.print_table("75").await;
    /// //dbg!(&csv_builder);
    ///
    /// // Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec![
    ///     "mobile".to_string(),
    ///     "name".to_string(),
    ///     "age".to_string(),
    ///     ];
    /// assert_eq!(csv_builder.get_headers().unwrap(), &expected_headers, "Headers do not match expected values.");
    ///
    ///
    /// let expected_data = vec![
    ///     vec!["9876543210".to_string(), "John Doe".to_string(), "28".to_string()],
    ///     vec!["9988776655".to_string(), "Emily Davis".to_string(), "32".to_string()],
    ///     vec!["8877665544".to_string(), "Another Invalid".to_string(), "45".to_string()],
    /// ];
    ///
    /// for (expected, actual) in expected_data.iter().zip(csv_builder.get_data().unwrap()) {
    ///     assert_eq!(expected[0], actual[0], "Count does not match expected values");
    ///     assert_eq!(expected[1], actual[1], "Count does not match expected values");
    ///     assert_eq!(expected[2], actual[2], "Count does not match expected values");
    /// }
    /// //assert_eq!(1,2);
    /// });
    /// ```
    pub async fn clean_or_test_clean_by_eliminating_rows_subject_to_column_parse_rules(
        &mut self,
        dask_cleaner_config: DaskCleanerConfig,
    ) -> &mut Self {
        //dbg!("BBBB");

        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            /*
            // Read and print the first 10 rows of the temporary file
            if let Ok(file) = File::open(temp_file.path()) {
                let reader = io::BufReader::new(file);
                for (index, line) in reader.lines().enumerate() {
                    if index < 10 {
                        if let Ok(line) = line {
                            println!("{}", line);
                        }
                    } else {
                        break;
                    }
                }
            } else {
                println!("Failed to open temp file for reading");
            }
            */

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                //dbg!(csv_path_str);
                match DaskConnect::dask_cleaner(csv_path_str, dask_cleaner_config.clone()).await {
                    Ok((headers, rows, report)) => {
                        //dbg!(&headers);

                        //dbg!(&rows);

                        if dask_cleaner_config.action == "CLEAN"
                            || dask_cleaner_config.action == "ANALYZE_AND_CLEAN"
                        {
                            self.headers = headers;
                            self.data = rows;
                        }

                        // Pretty print the report
                        if dask_cleaner_config.action == "ANALYZE"
                            || dask_cleaner_config.action == "ANALYZE_AND_CLEAN"
                        {
                            match serde_json::to_string_pretty(&report) {
                                Ok(pretty_report) => {
                                    println!("Cleanliness Report:\n{}", pretty_report);
                                }
                                Err(e) => {
                                    println!("Failed to pretty print report: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}", &e);
                        // Handle the error, possibly by logging or returning a default value
                        // For now, we just set empty headers and data
                        self.headers = vec![];
                        self.data = vec![];
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Sets the CSV header using an array of strings.
    pub fn set_header(&mut self, header: Vec<&str>) -> &mut Self {
        // Convert the header slice into a Vec<String>
        let header_row = header
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        // If there's an existing error, don't modify the builder
        if self.error.is_some() {
            return self;
        }

        // Assign the new headers
        self.headers = header_row;

        // If there's already data present, we need to ensure the headers are not duplicated.
        // Since headers are separate from data, we don't insert them into `data`.
        // The `headers` field will be written out when exporting the CSV.

        self
    }

    /// Replaces whitespaces in all headers with underscores.
    pub fn replace_header_whitespaces_with_underscores(&mut self) -> &mut Self {
        // If there's an existing error, don't modify the builder
        if self.error.is_some() {
            return self;
        }

        // Replace whitespaces in headers
        self.headers = self
            .headers
            .iter()
            .map(|s| s.replace(' ', "_"))
            .collect::<Vec<String>>();

        self
    }

    /// Adds a data row to the CSV.
    pub fn add_row(&mut self, row: Vec<&str>) -> &mut Self {
        if self.error.is_none() {
            let row_vec = row.into_iter().map(|s| s.to_string()).collect();
            self.data.push(row_vec);
        }
        self
    }

    /// Adds multiple data rows to the CSV.
    pub fn add_rows(&mut self, rows: Vec<Vec<&str>>) -> &mut Self {
        if self.error.is_none() {
            for row in rows {
                let row_vec = row.into_iter().map(|s| s.to_string()).collect();
                self.data.push(row_vec);
            }
        }
        self
    }

    /// Updates a data row at a specified index in the CSV.
    pub fn update_row_by_row_number(&mut self, index: usize, new_row: Vec<&str>) -> &mut Self {
        // Adjust for 1-based indexing
        let zero_based_index = index.saturating_sub(1);

        // Check if the 0-based index is within the range of data
        if zero_based_index < self.data.len() {
            // Update the row if the index is valid
            let row_vec = new_row.into_iter().map(|s| s.to_string()).collect();
            self.data[zero_based_index] = row_vec;
        } else {
            //dbg!(&index, &new_row);
            // Set error if the index is out of range
            self.error = Some(Box::new(std::io::Error::new(
                ErrorKind::InvalidInput,
                "Row index out of range",
            )));
        }

        self
    }

    /// Updates a data row by ID in the CSV, assuming the first column is 'id'.
    pub fn update_row_by_id(&mut self, index: usize, new_row: Vec<&str>) {
        let zero_based_index = index.saturating_sub(1);

        if zero_based_index < self.data.len() {
            // Update the row if the index is valid
            self.data[zero_based_index] = new_row.into_iter().map(|s| s.to_string()).collect();
        } else {
            eprintln!("Row index out of range. Cannot update row.");
        }
    }

    /// Deletes a data row at a specified index in the CSV.
    pub fn delete_row_by_row_number(&mut self, index: usize) -> bool {
        // Adjust for 1-based indexing
        let zero_based_index = index.saturating_sub(1);

        // Check if the 0-based index is within the range of data
        if zero_based_index < self.data.len() {
            self.data.remove(zero_based_index);
            true
        } else {
            false
        }
    }

    /// Deletes a data row by ID in the CSV, assuming the first column is 'id'.
    pub fn delete_row_by_id(&mut self, id: &str) -> bool {
        // Find the index of the row with the given id
        if let Some((index, _)) = self
            .data
            .iter()
            .enumerate()
            .find(|(_, row)| row.first().map_or(false, |first| first == id))
        {
            self.data.remove(index);
            true
        } else {
            false
        }
    }

    /// Adds column header
    pub fn add_column_header(&mut self, column_name: &str) -> &mut Self {
        if self.error.is_none() {
            self.headers.push(column_name.to_string());

            // Initialize the values of the new column to empty strings for existing rows
            for row in &mut self.data {
                row.push("".to_string());
            }
        }
        self
    }

    /// Adds multiple column headers
    pub fn add_column_headers(&mut self, column_names: Vec<&str>) -> &mut Self {
        if self.error.is_none() {
            for &column_name in column_names.iter() {
                self.headers.push(column_name.to_string());
            }

            // Initialize the values of the new columns to empty strings for existing rows
            for row in &mut self.data {
                for _ in &column_names {
                    row.push("".to_string());
                }
            }
        }

        self
    }

    pub fn order_columns(&mut self, order: Vec<&str>) -> &mut Self {
        // Clone the headers for creating column_map
        let headers_for_map = self.headers.clone();

        // Create a map from column names in headers to their indices
        let column_map: HashMap<&str, usize> = headers_for_map
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();

        //dbg!(&headers_for_map, &column_map);

        let mut start_columns = Vec::new();
        let mut end_columns = Vec::new();
        let mut middle_columns = self.headers.clone(); // Clone headers for middle_columns
        let mut specified_columns = HashSet::new();

        let mut at_start = true;
        for &item in &order {
            if item == "..." {
                at_start = false;
                continue;
            }

            if let Some(&index) = column_map.get(item) {
                if at_start {
                    start_columns.push(self.headers[index].clone());
                } else {
                    end_columns.push(self.headers[index].clone());
                }
                specified_columns.insert(item);
            }
        }

        middle_columns.retain(|col| !specified_columns.contains(col.as_str()));

        let reordered_header = [start_columns, middle_columns, end_columns].concat();
        //dbg!(&reordered_header);

        // Update the headers
        self.headers = reordered_header.clone(); // Clone reordered_header for self.headers

        let reordered_data = self
            .data
            .iter()
            .map(|row| {
                reordered_header
                    .iter()
                    .map(|col_name| {
                        let col_name_str = col_name.as_str();
                        // Find the original index of the column based on the new header arrangement
                        // and clone the value from the row at that index.
                        row[column_map[col_name_str]].clone()
                    })
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<Vec<String>>>();

        // Directly update self.data with the reordered data, without adding headers as a row
        self.data = reordered_data;

        self
    }

    /// Prints the column names of the CSV data, and returns self
    pub fn print_columns(&mut self) -> &mut Self {
        println!();
        for header in &self.headers {
            println!("{}", header);
        }
        self
    }

    /// Prints the number of data rows in the CSV.
    pub fn print_row_count(&mut self) -> &mut Self {
        // The number of rows is the length of the data vector.
        // Assuming the first row is the header and is not included in the count.
        let row_count = self.data.len();
        println!();
        println!("Row count: {}", row_count);

        self
    }

    /// Helper function to print a row in a JSON-like format.
    fn print_row_json(&self, row: &[String]) {
        println!("{{");
        for (header, value) in self.headers.iter().zip(row.iter()) {
            println!("  \"{}\": \"{}\",", header, value);
        }
        println!("}}");
    }

    /// Prints the first row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_first_row_small_file();
    /// //assert_eq!(1,2);
    ///
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub fn print_first_row_small_file(&mut self) -> &mut Self {
        if let Some((index, first_row)) = self.data.iter().enumerate().next() {
            let mut row_map = serde_json::Map::new();

            for (i, value) in first_row.iter().enumerate() {
                if let Some(header) = self.headers.get(i) {
                    row_map.insert(header.clone(), json!(value));
                }
            }

            let result = json!({ "rows": { (index + 1).to_string(): row_map } });
            println!("{}", serde_json::to_string_pretty(&result).unwrap());
        } else {
            println!("CSV is empty.");
        }
        self
    }

    /// Prints the first row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];      
    ///     
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///     
    /// let result_str = builder.print_first_row_big_file().await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_first_row_big_file(&mut self) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, "GET_FIRST_ROW").await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the first row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_first_row("75").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_first_row(&mut self, size_threshold: &str) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.print_first_row_big_file().await
        } else {
            self.print_first_row_small_file()
        }
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_last_row_small_file();
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub fn print_last_row_small_file(&mut self) -> &mut Self {
        if let Some((index, last_row)) = self.data.iter().enumerate().last() {
            let mut row_map = serde_json::Map::new();

            for (i, value) in last_row.iter().enumerate() {
                if let Some(header) = self.headers.get(i) {
                    row_map.insert(header.clone(), json!(value));
                }
            }

            let result = json!({ "rows": { (index + 1).to_string(): row_map } });
            println!("{}", serde_json::to_string_pretty(&result).unwrap());
        } else {
            println!("CSV is empty.");
        }
        self
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];      
    ///     
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///     
    /// let result_str = builder.print_last_row_big_file().await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_last_row_big_file(&mut self) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, "GET_LAST_ROW").await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_last_row("75").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_last_row(&mut self, size_threshold: &str) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.print_last_row_big_file().await
        } else {
            self.print_last_row_small_file()
        }
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_rows_range_small_file("2", "3");
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    /// Prints rows within a specified range from the CSV data.
    pub fn print_rows_range_small_file(&mut self, start: &str, end: &str) -> &mut Self {
        // Parse the start and end strings into usize, defaulting to 1 if parsing fails
        let start_index = usize::from_str(start).unwrap_or(1);
        let end_index = usize::from_str(end).unwrap_or(self.data.len());

        // Adjust the start index to align with internal zero-based indexing
        let adjusted_start = start_index.saturating_sub(1);
        // No need to adjust the end index as the range is exclusive

        let rows = self.data.get(adjusted_start..end_index).unwrap_or(&[]);

        let mut rows_vec: Vec<(usize, HashMap<String, String>)> = Vec::new();

        for (offset, row) in rows.iter().enumerate() {
            // Adjust the index for display as one-based
            let display_index = adjusted_start + offset + 1;
            let mut row_map = HashMap::new();
            for (header, value) in self.headers.iter().zip(row) {
                row_map.insert(header.clone(), value.clone());
            }
            rows_vec.push((display_index, row_map));
        }

        // Sort the rows_vec by the display_index
        rows_vec.sort_by_key(|&(index, _)| index);

        // Manually construct JSON output to maintain order
        let mut output = String::new();
        output.push_str("{\n  \"rows\": {\n");
        for (i, (index, row_map)) in rows_vec.iter().enumerate() {
            let comma = if i == rows_vec.len() - 1 { "" } else { "," };
            output.push_str(&format!("    \"{}\": {{\n", index));
            for (j, (key, value)) in row_map.iter().enumerate() {
                let key_comma = if j == row_map.len() - 1 { "" } else { "," };
                output.push_str(&format!("      \"{}\": \"{}\"{}", key, value, key_comma));
                output.push_str("\n");
            }
            output.push_str(&format!("    }}{}", comma));
            output.push_str("\n");
        }
        output.push_str("  }\n}");

        println!("{}", output);

        self
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];      
    ///     
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///     
    /// let result_str = builder.print_rows_range_big_file("2", "3").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_rows_range_big_file(&mut self, start: &str, end: &str) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let range_string = format!("GET_ROW_RANGE:{}_{}", start, end);

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, &range_string).await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_rows_range("2", "3", "75").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    /// Prints a range of rows by choosing the appropriate method based on the size of the self object.
    pub async fn print_rows_range(
        &mut self,
        start: &str,
        end: &str,
        size_threshold: &str,
    ) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.print_rows_range_big_file(start, end).await
        } else {
            self.print_rows_range_small_file(start, end)
        }
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];      
    ///     
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///     
    /// let result_str = builder.print_first_n_rows_small_file("2");
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub fn print_first_n_rows_small_file(&mut self, n: &str) -> &mut Self {
        self.print_rows_range_small_file("1", n);
        self
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];      
    ///     
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///     
    /// let result_str = builder.print_first_n_rows_big_file("2").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_first_n_rows_big_file(&mut self, n: &str) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let n_string = format!("GET_FIRST_N_ROWS:{}", n);

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, &n_string).await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```         
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///     
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_first_n_rows("2", "75").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_first_n_rows(&mut self, n: &str, size_threshold: &str) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.print_first_n_rows_big_file(n).await
        } else {
            self.print_first_n_rows_small_file(n)
        }
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];      
    ///     
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///     
    /// let result_str = builder.print_last_n_rows_small_file("2");
    /// // assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub fn print_last_n_rows_small_file(&mut self, n: &str) -> &mut Self {
        match n.parse::<usize>() {
            Ok(n) => {
                let start = if self.data.len() > n {
                    self.data.len() - n
                } else {
                    0
                };
                self.print_rows_range_small_file(
                    &format!("{}", start + 1),
                    &format!("{}", self.data.len()),
                );
            }
            Err(_) => println!("Invalid number: {}", n),
        }
        self
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];      
    ///     
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///     
    /// let result_str = builder.print_last_n_rows_big_file("2").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_last_n_rows_big_file(&mut self, n: &str) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let n_string = format!("GET_LAST_N_ROWS:{}", n);

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, &n_string).await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the last row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_last_n_rows("2", "75").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_last_n_rows(&mut self, n: &str, size_threshold: &str) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.print_last_n_rows_big_file(n).await
        } else {
            self.print_last_n_rows_small_file(n)
        }
    }

    /// Prints all rows of the CSV data.
    pub fn print_rows(&mut self) -> &mut Self {
        println!();
        for (index, row) in self.data.iter().enumerate() {
            // Adjust the index for display as one-based
            let display_index = index + 1;
            println!("Row {}: ", display_index);
            self.print_row_json(row);
        }

        // Print the total count of rows
        println!("\nTotal rows: {}", self.data.len());
        self
    }

    /// Prints rows matching the filter criteria and returns a new instance for chaining.
    pub fn print_rows_where(
        &self, // Immutable reference to retain the original state
        expressions: Vec<(&str, Exp)>,
        result_expression: &str,
    ) -> &Self {
        // Use the headers directly since we are not modifying data
        let headers = &self.headers;

        let mut row_number = 0; // Variable to keep track of the row number
        let mut printed_row_count = 0; // Variable to count the number of printed rows

        for row in self.data.iter() {
            row_number += 1; // Increment the row number for each row
            let mut expr_results = HashMap::new();
            expr_results.insert("true", true);
            expr_results.insert("false", false);

            // Evaluate each expression
            for (expr_name, exp) in &expressions {
                if let Some(column_index) = headers.iter().position(|h| h == &exp.column) {
                    if let Some(cell_value) = row.get(column_index) {
                        let result = match &exp.compare_with {
                            ExpVal::STR(value_str) => {
                                value_str.apply(cell_value, &exp.operator, &exp.compare_as)
                            }
                            ExpVal::VEC(values) => {
                                //let value_refs: Vec<&str> = values.iter().map(String::as_str).collect();
                                values.apply(cell_value, &exp.operator, &exp.compare_as)
                            }
                        };
                        expr_results.insert(*expr_name, result);
                    } else {
                        expr_results.insert(*expr_name, false);
                    }
                } else {
                    println!("Column '{}' not found in headers.", exp.column);
                    expr_results.insert(*expr_name, false);
                }
            }

            // Evaluate the final result expression
            if self.evaluate_result_expression(&expr_results, result_expression) {
                println!("Row number: {}", row_number); // Print the row number
                self.print_row_json(row); // Print the row if the result expression evaluates to true
                printed_row_count += 1; // Increment the count of printed rows
            }
        }

        println!("Total rows printed: {}", printed_row_count); // Print the total count of rows printed at the end

        self // Return the new instance
    }

    /// Prints specified cells for each row of the CSV data.
    pub fn print_cells(&mut self, columns: Vec<&str>) -> &mut Self {
        println!();
        // First, determine the indices of the specified columns
        let column_indices: Vec<Option<usize>> = columns
            .iter()
            .map(|&col| self.headers.iter().position(|h| h == col))
            .collect();

        for row in &self.data {
            println!();
            for (col_name, col_index) in columns.iter().zip(&column_indices) {
                if let Some(index) = col_index {
                    // Safely get the value from the row
                    if let Some(value) = row.get(*index) {
                        println!("\"{}\": \"{}\",", col_name, value);
                    }
                } else {
                    println!("\"{}\": column not found,", col_name);
                }
            }
        }
        self
    }

    /// Prints the table by choosing the appropriate method based on the size of the self object.
    pub async fn print_table(&mut self, size_threshold: &str) -> &mut Self {
        // Nested function to calculate the size of the self object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.print_table_big_file().await
        } else {
            self.print_table_small_file()
        }
    }

    /// Prints the first row of the CSV data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string(), "email".to_string(), "phone".to_string(), "address".to_string(), "city".to_string(), "state".to_string(), "country".to_string(), "zipcode".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "alice@example.com".to_string(), "123-456-7890".to_string(), "123 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62701".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string(), "bob@example.com".to_string(), "123-456-7891".to_string(), "124 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62702".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string(), "charlie@example.com".to_string(), "123-456-7892".to_string(), "125 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62703".to_string()],
    ///     vec!["4".to_string(), "David".to_string(), "28".to_string(), "david@example.com".to_string(), "123-456-7893".to_string(), "126 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62704".to_string()],
    ///     vec!["5".to_string(), "Eve".to_string(), "22".to_string(), "eve@example.com".to_string(), "123-456-7894".to_string(), "127 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62705".to_string()],
    ///     vec!["6".to_string(), "Frank".to_string(), "33".to_string(), "frank@example.com".to_string(), "123-456-7895".to_string(), "128 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62706".to_string()],
    ///     vec!["7".to_string(), "Grace".to_string(), "27".to_string(), "grace@example.com".to_string(), "123-456-7896".to_string(), "129 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62707".to_string()],
    ///     vec!["8".to_string(), "Hank".to_string(), "31".to_string(), "hank@example.com".to_string(), "123-456-7897".to_string(), "130 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62708".to_string()],
    ///     vec!["9".to_string(), "Ivy".to_string(), "26".to_string(), "ivy@example.com".to_string(), "123-456-7898".to_string(), "131 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62709".to_string()],
    ///     vec!["10".to_string(), "Jack".to_string(), "29".to_string(), "jack@example.com".to_string(), "123-456-7899".to_string(), "132 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62710".to_string()],
    ///     vec!["11".to_string(), "Karen".to_string(), "34".to_string(), "karen@example.com".to_string(), "123-456-7900".to_string(), "133 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62711".to_string()],
    ///     vec!["12".to_string(), "Leo".to_string(), "21".to_string(), "leo@example.com".to_string(), "123-456-7901".to_string(), "134 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62712".to_string()],
    ///     vec!["13".to_string(), "Mona".to_string(), "32".to_string(), "mona@example.com".to_string(), "123-456-7902".to_string(), "135 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62713".to_string()],
    ///     vec!["14".to_string(), "Nina".to_string(), "23".to_string(), "nina@example.com".to_string(), "123-456-7903".to_string(), "136 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62714".to_string()],
    ///     vec!["15".to_string(), "Oscar".to_string(), "36".to_string(), "oscar@example.com".to_string(), "123-456-7904".to_string(), "137 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62715".to_string()],
    ///     vec!["16".to_string(), "Paul".to_string(), "24".to_string(), "paul@example.com".to_string(), "123-456-7905".to_string(), "138 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62716".to_string()],
    ///     vec!["17".to_string(), "Quinn".to_string(), "37".to_string(), "quinn@example.com".to_string(), "123-456-7906".to_string(), "139 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62717".to_string()],
    ///     vec!["18".to_string(), "Rita".to_string(), "25".to_string(), "rita@example.com".to_string(), "123-456-7907".to_string(), "140 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62718".to_string()],
    ///     vec!["19".to_string(), "Sam".to_string(), "28".to_string(), "sam@example.com".to_string(), "123-456-7908".to_string(), "141 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62719".to_string()],
    ///     vec!["20".to_string(), "Tina".to_string(), "29".to_string(), "tina@example.com".to_string(), "123-456-7909".to_string(), "142 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62720".to_string()],
    ///     vec!["21".to_string(), "Uma".to_string(), "26".to_string(), "uma@example.com".to_string(), "123-456-7910".to_string(), "143 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62721".to_string()],
    ///     vec!["22".to_string(), "Vince".to_string(), "30".to_string(), "vince@example.com".to_string(), "123-456-7911".to_string(), "144 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62722".to_string()],
    ///     vec!["23".to_string(), "Wendy".to_string(), "33".to_string(), "wendy@example.com".to_string(), "123-456-7912".to_string(), "145 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62723".to_string()],
    ///     vec!["24".to_string(), "Xander".to_string(), "34".to_string(), "xander@example.com".to_string(), "123-456-7913".to_string(), "146 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62724".to_string()],
    ///     vec!["25".to_string(), "Yara".to_string(), "31".to_string(), "yara@example.com".to_string(), "123-456-7914".to_string(), "147 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62725".to_string()],
    ///     vec!["26".to_string(), "Zane".to_string(), "27".to_string(), "zane@example.com".to_string(), "123-456-7915".to_string(), "148 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62726".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.print_table_big_file().await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn print_table_big_file(&mut self) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, "GET_SUMMARY").await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        //println!("Result:\n{}", pretty_res);

                        // Parse the JSON response
                        if let Ok(parsed_res) =
                            serde_json::from_str::<HashMap<String, Value>>(&pretty_res)
                        {
                            if let Some(rows) = parsed_res.get("rows").and_then(|v| v.as_object()) {
                                // Headers
                                let headers = self.headers.clone();
                                let show_rows = 5;
                                let max_cell_width: usize = 45;
                                let max_columns = 8; // Total columns including the <<+X cols>> column and the last 3 columns

                                // Collect and sort the row keys
                                let mut sorted_keys: Vec<usize> = rows
                                    .keys()
                                    .filter_map(|k| k.parse::<usize>().ok())
                                    .collect();
                                sorted_keys.sort();

                                let total_rows = *sorted_keys.last().unwrap_or(&0);
                                let displayed_rows = sorted_keys.len();
                                let omitted_row_count = total_rows - displayed_rows;

                                // Calculate the maximum length for each column based on visible rows
                                let mut max_lengths =
                                    headers.iter().map(|h| h.len() + 1).collect::<Vec<usize>>();
                                for &key in sorted_keys.iter().take(show_rows).chain(
                                    sorted_keys
                                        .iter()
                                        .skip(displayed_rows.saturating_sub(show_rows)),
                                ) {
                                    if let Some(row) =
                                        rows.get(&key.to_string()).and_then(|v| v.as_object())
                                    {
                                        for (i, header) in headers.iter().enumerate() {
                                            if let Some(cell) =
                                                row.get(header).and_then(|v| v.as_str())
                                            {
                                                let current_max =
                                                    std::cmp::max(max_lengths[i], cell.len());
                                                max_lengths[i] =
                                                    std::cmp::min(current_max, max_cell_width);
                                            }
                                        }
                                    }
                                }

                                // Detect omitted columns
                                let total_columns = headers.len();
                                let omitted_column_count = if total_columns > max_columns {
                                    total_columns - (max_columns - 1) // Last 3 columns plus the <<+X cols>> column should be shown
                                } else {
                                    0
                                };

                                // Truncate headers and max lengths for display
                                let headers_to_display = if total_columns > max_columns {
                                    headers
                                        .iter()
                                        .take(max_columns - 4)
                                        .cloned()
                                        .chain(std::iter::once(format!(
                                            "  <<+{} cols>> ",
                                            omitted_column_count
                                        )))
                                        .chain(headers.iter().skip(total_columns - 3).cloned())
                                        .collect::<Vec<_>>()
                                } else {
                                    headers.clone()
                                };

                                let max_lengths_to_display = if total_columns > max_columns {
                                    max_lengths
                                        .iter()
                                        .take(max_columns - 4)
                                        .cloned()
                                        .chain(std::iter::once(15)) // Ensure <<+X cols>> is aligned
                                        .chain(max_lengths.iter().skip(total_columns - 3).cloned())
                                        .collect::<Vec<_>>()
                                } else {
                                    max_lengths.clone()
                                };

                                // Function to truncate and pad string based on column max length
                                let format_cell = |s: &str, max_length: usize| -> String {
                                    format!("{:width$}", s, width = max_length)
                                };

                                // Function to create the row for omitted columns placeholder
                                let format_omitted_cell = |max_length: usize| -> String {
                                    format!("{:width$}", "...", width = max_length)
                                };

                                // Print the headers
                                let header_line = headers_to_display
                                    .iter()
                                    .zip(max_lengths_to_display.iter())
                                    .map(|(header, &max_length)| format_cell(header, max_length))
                                    .collect::<Vec<String>>()
                                    .join("|");
                                println!("\n|{}|", header_line);
                                println!("{}", "-".repeat(header_line.len() + 2));

                                // Print function for rows
                                let print_row =
                                    |row: &Map<String, Value>, _max_lengths: &Vec<usize>| {
                                        let row_line = headers_to_display
                                            .iter()
                                            .zip(max_lengths_to_display.iter())
                                            .map(|(header, &max_length)| {
                                                if header.starts_with("  <<+") {
                                                    format_omitted_cell(max_length)
                                                } else {
                                                    row.get(header)
                                                        .and_then(|v| v.as_str())
                                                        .map(|cell| format_cell(cell, max_length))
                                                        .unwrap_or_else(|| {
                                                            format_cell("...", max_length)
                                                        })
                                                }
                                            })
                                            .collect::<Vec<String>>()
                                            .join("|");
                                        println!("|{}|", row_line);
                                    };

                                // Print the first `show_rows`
                                for &key in sorted_keys.iter().take(show_rows) {
                                    if let Some(row) =
                                        rows.get(&key.to_string()).and_then(|v| v.as_object())
                                    {
                                        print_row(row, &max_lengths_to_display);
                                    }
                                }

                                // Print the ellipsis and omitted row count
                                if total_rows > 2 * show_rows {
                                    let row_word = if omitted_row_count == 1 {
                                        "row"
                                    } else {
                                        "rows"
                                    };
                                    println!("<<+{} {}>>", omitted_row_count, row_word);

                                    // Print the last `show_rows`
                                    for &key in sorted_keys.iter().rev().take(show_rows).rev() {
                                        if let Some(row) =
                                            rows.get(&key.to_string()).and_then(|v| v.as_object())
                                        {
                                            print_row(row, &max_lengths_to_display);
                                        }
                                    }
                                } else if displayed_rows > show_rows {
                                    for &key in sorted_keys
                                        .iter()
                                        .skip(show_rows)
                                        .take(displayed_rows - show_rows)
                                    {
                                        if let Some(row) =
                                            rows.get(&key.to_string()).and_then(|v| v.as_object())
                                        {
                                            print_row(row, &max_lengths_to_display);
                                        }
                                    }
                                }

                                // Print omitted columns
                                if omitted_column_count > 0 {
                                    let omitted_columns: Vec<String> =
                                        headers.iter().skip(max_columns - 4).cloned().collect();
                                    println!("\nOmitted columns: {}", omitted_columns.join(", "));
                                }

                                // Print total number of rows
                                println!("Total rows: {}", total_rows);
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints an abbreviated table of the CSV data with lines and consistent spacing for cells.
    pub fn print_table_small_file(&mut self) -> &mut Self {
        let show_rows = 5; // Number of rows to show at the start and end
        let total_rows = self.data.len();
        let max_cell_width: usize = 45; // Max width for any cell

        // Calculate the maximum length for each column based on visible rows
        let mut max_lengths = self
            .headers
            .iter()
            .map(|h| h.len() + 1)
            .collect::<Vec<usize>>();
        for row in self
            .data
            .iter()
            .take(show_rows)
            .chain(self.data.iter().skip(total_rows.saturating_sub(show_rows)))
        {
            for (i, cell) in row.iter().enumerate() {
                let current_max = std::cmp::max(max_lengths[i], cell.len());
                max_lengths[i] = std::cmp::min(current_max, max_cell_width);
            }
        }

        // Function to truncate and pad string based on column max length
        let format_cell = |s: &String, max_length: usize| -> String {
            format!("{:width$.width$}", s, width = max_length)
        };

        // Determine headers to print and omitted columns
        let (headers_to_print, omitted_columns) = if self.headers.len() > 7 {
            let omitted_count = self.headers.len() - 7;
            let column_word = if omitted_count == 1 { "col" } else { "cols" };
            let ellipsis_text = format!("  <<+{} {}>> ", omitted_count, column_word);
            let combined_headers = [
                &self.headers[..4],
                &vec![ellipsis_text],
                &self.headers[self.headers.len() - 3..],
            ]
            .concat();
            (combined_headers, &self.headers[4..self.headers.len() - 3])
        } else {
            (self.headers.clone(), &[] as &[String])
        };

        // Adjust max_lengths array according to headers_to_print
        let adjusted_max_lengths = if self.headers.len() > 7 {
            let mut lengths = max_lengths[..4].to_vec();
            lengths.push(15); // Assigning an appropriate length for the ellipsis placeholder
            lengths.extend_from_slice(&max_lengths[max_lengths.len() - 3..]);
            lengths
        } else {
            max_lengths
        };

        // Calculate total table width
        let table_width = adjusted_max_lengths
            .iter()
            .map(|&len| len + 1)
            .sum::<usize>()
            + 1;

        // Print the headers
        println!(
            "\n|{}|",
            headers_to_print
                .iter()
                .zip(adjusted_max_lengths.iter())
                .map(|(header, &max_length)| format_cell(header, max_length))
                .collect::<Vec<String>>()
                .join("|")
        );
        println!("{}", "-".repeat(table_width));

        // Print function for rows
        let print_row = |row: &Vec<String>, max_lengths: &Vec<usize>| {
            let row_to_print = if row.len() > 7 {
                let mut cells = row[..4].to_vec();
                cells.push("...".to_string()); // Inserting ellipsis in the row
                cells.extend_from_slice(&row[row.len() - 3..]);
                cells
            } else {
                row.clone()
            };

            println!(
                "|{}|",
                row_to_print
                    .iter()
                    .zip(max_lengths.iter())
                    .map(|(cell, &max_length)| format_cell(cell, max_length))
                    .collect::<Vec<String>>()
                    .join("|")
            );
        };

        // Print the first `show_rows`
        for row in self.data.iter().take(show_rows) {
            print_row(row, &adjusted_max_lengths);
        }

        // Check if ellipsis and bottom rows are needed for data
        if total_rows > 2 * show_rows {
            let omitted_row_count = total_rows - 2 * show_rows;
            let row_word = if omitted_row_count == 1 {
                "row"
            } else {
                "rows"
            };

            println!("<<+{} {}>>", omitted_row_count, row_word);
            for row in self.data.iter().skip(total_rows - show_rows) {
                print_row(row, &adjusted_max_lengths);
            }
        } else if total_rows > show_rows {
            for row in self
                .data
                .iter()
                .skip(show_rows)
                .take(total_rows - show_rows)
            {
                print_row(row, &adjusted_max_lengths);
            }
        }

        if !omitted_columns.is_empty() {
            let omitted_column_names = omitted_columns.join(", ");
            println!("\nOmitted columns: {}", omitted_column_names);
        }

        // Print total number of rows
        println!("Total rows: {}", total_rows);

        self
    }

    /// Prints the full table of the CSV data with lines and consistent spacing for cells.
    pub fn print_table_all_rows(&mut self) -> &mut Self {
        let total_rows = self.data.len();
        let max_cell_width: usize = 45; // Max width for any cell

        // Calculate the maximum length for each column based on all rows
        let mut max_lengths = self
            .headers
            .iter()
            .map(|h| h.len() + 1)
            .collect::<Vec<usize>>();
        for row in self.data.iter() {
            for (i, cell) in row.iter().enumerate() {
                let current_max = std::cmp::max(max_lengths[i], cell.len());
                max_lengths[i] = std::cmp::min(current_max, max_cell_width);
            }
        }

        // Function to truncate and pad string based on column max length
        let format_cell = |s: &String, max_length: usize| -> String {
            format!("{:width$.width$}", s, width = max_length)
        };

        // Determine headers to print and omitted columns
        let headers_to_print = if self.headers.len() > 7 {
            let omitted_count = self.headers.len() - 7;
            let column_word = if omitted_count == 1 { "col" } else { "cols" };
            let ellipsis_text = format!("  <<+{} {}>> ", omitted_count, column_word);
            let combined_headers = [
                &self.headers[..4],
                &vec![ellipsis_text],
                &self.headers[self.headers.len() - 3..],
            ]
            .concat();
            combined_headers
        } else {
            self.headers.clone()
        };

        // Adjust max_lengths array according to headers_to_print
        let adjusted_max_lengths = if self.headers.len() > 7 {
            let mut lengths = max_lengths[..4].to_vec();
            lengths.push(15); // Assigning an appropriate length for the ellipsis placeholder
            lengths.extend_from_slice(&max_lengths[max_lengths.len() - 3..]);
            lengths
        } else {
            max_lengths
        };

        // Calculate total table width
        let table_width = adjusted_max_lengths
            .iter()
            .map(|&len| len + 1)
            .sum::<usize>()
            + 1;

        // Print the headers
        println!(
            "\n|{}|",
            headers_to_print
                .iter()
                .zip(adjusted_max_lengths.iter())
                .map(|(header, &max_length)| format_cell(header, max_length))
                .collect::<Vec<String>>()
                .join("|")
        );
        println!("{}", "-".repeat(table_width));

        let print_row = |row: &Vec<String>, max_lengths: &Vec<usize>, headers_count: usize| {
            let mut row_to_print = Vec::new();
            if headers_count > 7 {
                for i in 0..4 {
                    // Add the first 4 columns
                    row_to_print.push(row[i].clone());
                }
                // Add the ellipsis placeholder for omitted columns
                row_to_print.push("...".to_string());
                for i in (headers_count - 3)..headers_count {
                    // Add the last 3 columns, adjusting index as needed
                    row_to_print.push(row[i].clone());
                }
            } else {
                // If there are 7 or fewer headers, just add all columns without omission
                row_to_print.extend_from_slice(row);
            }
            println!(
                "|{}|",
                row_to_print
                    .iter()
                    .zip(max_lengths.iter())
                    .map(|(cell, &max_length)| format_cell(cell, max_length))
                    .collect::<Vec<String>>()
                    .join("|")
            );
        };

        for row in self.data.iter() {
            print_row(row, &adjusted_max_lengths, self.headers.len());
        }

        // Print total number of rows
        println!("Total rows: {}", total_rows);

        self
    }

    /// Prints the frequency of each unique value in specified columns, sorted by numbers or timestamps if possible.
    /// If values cannot be parsed as numbers or timestamps, they are sorted in ascending alphabetical order.
    /// This method is useful for quickly analyzing the distribution of data within specific columns of a dataset.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of strings representing the names of the columns for which frequencies should be printed.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskFreqLinearConfig, DaskConnect};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///  let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let columns = "mobile, name";
    ///
    ///     let dask_freq_linear_config = DaskFreqLinearConfig {
    ///         column_names: columns.to_string(),
    ///         order_by: "FREQ_DESC".to_string(),
    ///         limit: "".to_string()
    ///     };
    ///
    /// let mut builder = CsvBuilder::from_csv(csv_path_str);
    /// builder.print_table("75").await.print_freq(dask_freq_linear_config).await;
    /// assert_eq!(1,1);
    /// //assert_eq!(1,2);
    /// });
    /// ```
    /// // This will output:
    ///
    /// Frequency for column 'event':
    ///   Independence Day: f = 1 (33.33%)
    ///   New Year         : f = 1 (33.33%)
    ///   New Year's Day   : f = 1 (33.33%)
    ///
    ///
    /// Note: This output assumes that the `print_freq` method properly calculates and formats frequencies as demonstrated.

    pub async fn print_freq(&mut self, dask_freq_linear_config: DaskFreqLinearConfig) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_freq_linear_stats(csv_path_str, dask_freq_linear_config)
                    .await
                {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the frequency of each unique value in specified columns, sorted by numbers or timestamps if possible.
    /// If values cannot be parsed as numbers or timestamps, they are sorted in ascending alphabetical order.
    /// This method is useful for quickly analyzing the distribution of data within specific columns of a dataset.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of strings representing the names of the columns for which frequencies should be printed.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskFreqCascadingConfig, DaskConnect};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///  let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let columns = "mobile, name";
    ///
    ///     let dask_freq_linear_config = DaskFreqCascadingConfig {
    ///         column_names: columns.to_string(),
    ///         order_by: "FREQ_DESC".to_string(),
    ///         limit: "".to_string()
    ///     };
    ///
    /// let mut builder = CsvBuilder::from_csv(csv_path_str);
    /// builder.print_table("75").await.print_freq_cascading(dask_freq_linear_config).await;
    /// assert_eq!(1,1);
    /// //assert_eq!(1,2);
    /// });
    /// ```
    /// // This will output:
    ///
    /// Frequency for column 'event':
    ///   Independence Day: f = 1 (33.33%)
    ///   New Year         : f = 1 (33.33%)
    ///   New Year's Day   : f = 1 (33.33%)
    ///
    ///
    /// Note: This output assumes that the `print_freq` method properly calculates and formats frequencies as demonstrated.

    pub async fn print_freq_cascading(
        &mut self,
        dask_freq_cascading_config: DaskFreqCascadingConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_freq_cascading_stats(
                    csv_path_str,
                    dask_freq_cascading_config,
                )
                .await
                {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the frequency of each unique value in specified columns, sorted by numbers or timestamps if possible.
    /// If values cannot be parsed as numbers or timestamps, they are sorted in ascending alphabetical order.
    /// This method is useful for quickly analyzing the distribution of data within specific columns of a dataset.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of strings representing the names of the columns for which frequencies should be printed.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskConnect;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///  let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let columns = "mobile, name";
    ///
    ///
    /// let mut builder = CsvBuilder::from_csv(csv_path_str);
    /// builder.print_table("75").await.print_unique_values_stats(&columns).await;
    /// assert_eq!(1,1);
    /// //assert_eq!(1,2);
    /// });
    /// ```
    /// // This will output:
    ///
    /// Frequency for column 'event':
    ///   Independence Day: f = 1 (33.33%)
    ///   New Year         : f = 1 (33.33%)
    ///   New Year's Day   : f = 1 (33.33%)
    ///
    ///
    /// Note: This output assumes that the `print_freq` method properly calculates and formats frequencies as demonstrated.

    pub async fn print_unique_values_stats(&mut self, columns: &str) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_unique_value_stats(csv_path_str, columns).await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the frequency of each unique value in specified columns of the CSV, with optional grouping.
    /// This function first tries to sort values as numbers or timestamps. If they
    /// can't be parsed as such, it then sorts them in ascending alphabetical order.
    /// Groupings allow multiple values to be considered as one for the purpose of frequency calculation.
    ///
    /// # Arguments
    ///
    /// * `columns_with_groupings` - A vector of tuples where each tuple consists of a column name
    ///   and a vector of groupings. Each grouping is a tuple where the first element is a primary value
    ///   and the second element is a vector of values to be grouped under the primary value.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    ///
    /// let mut builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string(), "event".to_string()],
    ///     vec![
    ///         vec!["2023-01-01".to_string(), "23.5".to_string(), "New Year".to_string()],
    ///         vec!["2023-01-01".to_string(), "23.5".to_string(), "Holiday".to_string()],
    ///         vec!["2023-01-02".to_string(), "24.1".to_string(), "Workday".to_string()],
    ///     ],
    /// );
    ///
    /// // The following call to print_freq_mapped demonstrates how to group multiple values under a primary key.
    /// // For instance, both "New Year" and "Holiday" events could be grouped under "Holiday Season" for frequency counting.
    /// builder.print_freq_mapped(vec![
    ///     ("event", vec![
    ///         ("Holiday Season", vec!["New Year", "Holiday"]),
    ///         ("Regular Day", vec!["Workday"])
    ///     ])
    /// ]);
    /// // This will count "New Year" and "Holiday" as "Holiday Season", and "Workday" as "Regular Day".
    /// // Output should sort values based on their grouping and then alphabetically or numerically within each group.
    /// // Frequency for column 'event':
    /// //   Holiday Season: 2
    /// //   Regular Day: 1
    /// ```
    ///
    /// This method does not capture output in this example; it demonstrates the syntax for calling the function.
    pub fn print_freq_mapped(
        &mut self,
        columns_with_groupings: Vec<(&str, Vec<(&str, Vec<&str>)>)>,
    ) -> &mut Self {
        fn parse_as_number_or_datetime(input: &str) -> (bool, f64) {
            if let Ok(num) = input.parse::<f64>() {
                (true, num) // First element true indicates it's a number
            } else {
                let formats = [
                    "%Y-%m-%d",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y/%m/%d",
                    "%d-%m-%Y",
                    "%Y-%m-%d %H:%M:%S%.f",
                    "%b %d, %Y",
                ];
                for fmt in formats.iter() {
                    if let Ok(date) = NaiveDateTime::parse_from_str(input, fmt) {
                        let date_utc = Utc.from_utc_datetime(&date);
                        return (true, date_utc.timestamp() as f64); // true indicates parsed as date
                    }
                }
                (false, 0.0) // false indicates not parsed
            }
        }

        for (col, groupings) in columns_with_groupings {
            let col_idx = if let Some(index) = self.headers.iter().position(|r| r == col) {
                index
            } else {
                println!("Column '{}' not found.", col);
                continue;
            };

            let mut value_map: HashMap<String, String> = HashMap::new();
            let mut apply_groupings = true;

            for (primary_value, values) in groupings {
                if primary_value == "NO_GROUPINGS" {
                    apply_groupings = false;
                    break;
                }
                for value in values {
                    value_map.insert(value.to_string(), primary_value.to_string());
                }
            }

            let mut freq_map: HashMap<String, usize> = HashMap::new();

            // Count the frequency of each unique value
            for row in &self.data {
                if let Some(value) = row.get(col_idx) {
                    let value_str = value.to_string();
                    let grouped_value = if apply_groupings {
                        value_map.get(&value_str).unwrap_or(&value_str)
                    } else {
                        &value_str
                    };
                    *freq_map.entry(grouped_value.to_string()).or_insert(0) += 1;
                }
            }

            // Sorting the frequency map by trying to parse as numbers or timestamps first
            let mut sorted_freq: Vec<(String, usize)> = freq_map.into_iter().collect();
            sorted_freq.sort_by(|a, b| {
                parse_as_number_or_datetime(&a.0)
                    .partial_cmp(&parse_as_number_or_datetime(&b.0))
                    .unwrap_or_else(|| a.0.cmp(&b.0))
            });

            // Print the frequencies
            println!("\nFrequency for column '{}':", self.headers[col_idx]);
            for (value, count) in sorted_freq {
                println!("{}: {}", value, count);
            }
        }

        self
    }

    /// Cascade sorts the data as per the indicated order of columns
    pub fn cascade_sort(&mut self, orders: Vec<(String, String)>) -> &mut Self {
        // Assuming `self.headers` and `self.data` are defined, and `self.data` is sortable

        let column_indices: HashMap<&str, usize> = self
            .headers
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();

        self.data.sort_by(|a, b| {
            let mut cmp = std::cmp::Ordering::Equal;
            for (column_name, order) in &orders {
                if let Some(&index) = column_indices.get(column_name.as_str()) {
                    let a_val = &a[index];
                    let b_val = &b[index];

                    cmp = if let (Ok(a_num), Ok(b_num)) =
                        (a_val.parse::<f64>(), b_val.parse::<f64>())
                    {
                        // Both values are numbers, compare as f64
                        if order == "ASC" {
                            a_num
                                .partial_cmp(&b_num)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        } else {
                            b_num
                                .partial_cmp(&a_num)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        }
                    } else {
                        // At least one value is not a number, compare as string
                        if order == "ASC" {
                            a_val.cmp(b_val)
                        } else {
                            b_val.cmp(a_val)
                        }
                    };

                    if cmp != std::cmp::Ordering::Equal {
                        break;
                    }
                }
            }
            cmp
        });

        self
    }

    /// Reverses the order of the rows in the data.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    ///
    /// let headers = vec!["col1_header".to_string(), "col2_header".to_string()];
    /// let data = vec![
    ///     vec!["row1_col1".to_string(), "row1_col2".to_string()],
    ///     vec!["row2_col1".to_string(), "row2_col2".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// builder = builder.reverse_rows();
    /// assert_eq!(
    ///     builder.get_data(),
    ///     Some(&vec![
    ///         vec!["row2_col1".to_string(), "row2_col2".to_string()],
    ///         vec!["row1_col1".to_string(), "row1_col2".to_string()],
    ///     ])
    /// );
    /// ```
    /*
        pub fn reverse_rows(mut self) -> Self {
            self.data.reverse();
            self
        }
    */
    pub fn reverse_rows(mut self) -> Self {
        let reversed_data: Vec<_> = self.data.into_par_iter().rev().collect();
        self.data = reversed_data;
        self
    }

    /// Reverses the order of the columns in the data, moving the corresponding data accordingly.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["col1_header".to_string(), "col2_header".to_string()];
    /// let data = vec![
    ///     vec!["row1_col1".to_string(), "row1_col2".to_string()],
    ///     vec!["row2_col1".to_string(), "row2_col2".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// builder = builder.reverse_columns();
    /// assert_eq!(
    ///     builder.get_headers(),
    ///     Some(&["col2_header".to_string(), "col1_header".to_string()][..])
    /// );
    /// assert_eq!(
    ///     builder.get_data(),
    ///     Some(&vec![
    ///         vec!["row1_col2".to_string(), "row1_col1".to_string()],
    ///         vec!["row2_col2".to_string(), "row2_col1".to_string()],
    ///     ])
    /// );
    /// ```
    pub fn reverse_columns(mut self) -> Self {
        self.headers.reverse();
        self.data.par_iter_mut().for_each(|row| {
            row.reverse();
        });
        self
    }

    /// Sets a particular column as an index starting from 1
    pub fn resequence_id_column(&mut self, column_name: &str) -> &mut Self {
        if let Some(column_index) = self.headers.iter().position(|h| h == column_name) {
            // Iterate through each row and update the column's value with the new sequence
            for (i, row) in self.data.iter_mut().enumerate() {
                if column_index < row.len() {
                    // Replace the value with the new sequence number, starting from 1
                    row[column_index] = (i + 1).to_string();
                }
            }
        }
        self
    }

    /// Drops specified columns from the CSV data.
    pub fn drop_columns(&mut self, columns: Vec<&str>) -> &mut Self {
        let columns_set: HashSet<&str> = columns.into_iter().collect();

        // Filter out the headers and indices of columns to be dropped
        let remaining_headers = self
            .headers
            .iter()
            .enumerate()
            .filter(|(_, h)| !columns_set.contains(h.as_str()))
            .map(|(i, h)| (i, h.clone()))
            .collect::<Vec<(usize, String)>>();

        // Rebuild the data without the dropped columns
        self.data = self
            .data
            .iter()
            .map(|row| {
                remaining_headers
                    .iter()
                    .map(|(i, _)| row[*i].clone())
                    .collect()
            })
            .collect();

        // Update headers
        self.headers = remaining_headers.into_iter().map(|(_, h)| h).collect();

        self
    }

    /// Retains only the columns specified and orders them.
    pub fn retain_columns(&mut self, columns_to_retain: Vec<&str>) -> &mut Self {
        if self.error.is_some() {
            return self;
        }

        // Map of header name to its index for efficient lookups
        let header_map: HashMap<&str, usize> = self
            .headers
            .iter()
            .enumerate()
            .map(|(i, header)| (header.as_str(), i))
            .collect();

        // Filter and order headers based on 'columns_to_retain', preserving order
        let retained_headers: Vec<String> = columns_to_retain
            .iter()
            .filter_map(|&col| {
                if header_map.contains_key(col) {
                    Some(col.to_string())
                } else {
                    None
                }
            })
            .collect();

        // Rebuild data rows to include only the retained columns, in the order specified
        let retained_data: Vec<Vec<String>> = self
            .data
            .iter()
            .map(|row| {
                columns_to_retain
                    .iter()
                    .filter_map(|&col| header_map.get(col).and_then(|&idx| row.get(idx).cloned()))
                    .collect()
            })
            .collect();

        self.headers = retained_headers;
        self.data = retained_data;

        self
    }

    pub fn drop_rows(
        &mut self,
        start: &str,
        range_indicator: &str,
        end: &str,
        single: &str,
    ) -> &mut Self {
        let mut rows_set = HashSet::new();

        // Check if range_indicator is "..."
        if range_indicator == "..." {
            if let (Ok(start_index), Ok(end_index)) = (start.parse::<usize>(), end.parse::<usize>())
            {
                // Adjust for 1-based indexing and include the range
                for i in (start_index.saturating_sub(1))..(end_index) {
                    rows_set.insert(i);
                }
            }
        }

        // Parse and include the single row index
        if let Ok(single_index) = single.parse::<usize>() {
            rows_set.insert(single_index.saturating_sub(1));
        }

        // Filter out the rows to be dropped
        self.data = self
            .data
            .iter()
            .enumerate()
            .filter(|(i, _)| !rows_set.contains(i))
            .map(|(_, row)| row.clone())
            .collect();

        self
    }

    /// Renames specified columns in the CSV data.
    pub fn rename_columns(&mut self, renames: Vec<(&str, &str)>) -> &mut Self {
        let rename_map: HashMap<&str, &str> = renames.into_iter().collect();

        self.headers = self
            .headers
            .iter()
            .map(|h| {
                // Convert &String to &str for the unwrap_or part
                let h_str = h.as_str();
                rename_map.get(h_str).unwrap_or(&h_str).to_string()
            })
            .collect();

        self
    }

    pub fn where_(&mut self, expressions: Vec<(&str, Exp)>, result_expression: &str) -> &mut Self {
        // Clone headers and data to avoid borrowing issues
        let headers_clone = self.headers.clone();

        // First, drain the data to get ownership of the rows
        let mut drained_data = self.data.drain(..).collect::<Vec<_>>();

        // Filter data based on the given expressions
        let filtered_data = drained_data
            .drain(..)
            .filter(|row| {
                let mut expr_results = HashMap::new();
                expr_results.insert("true", true);
                expr_results.insert("false", false);

                // Evaluate each expression
                for (expr_name, exp) in &expressions {
                    if let Some(column_index) = headers_clone.iter().position(|h| h == &exp.column)
                    {
                        if let Some(cell_value) = row.get(column_index) {
                            let result = match &exp.compare_with {
                                ExpVal::STR(value_str) => {
                                    value_str.apply(cell_value, &exp.operator, &exp.compare_as)
                                }
                                ExpVal::VEC(values) => {
                                    values.apply(cell_value, &exp.operator, &exp.compare_as)
                                }
                            };
                            expr_results.insert(*expr_name, result);
                        } else {
                            expr_results.insert(*expr_name, false);
                        }
                    } else {
                        println!("Column '{}' not found in headers.", exp.column);
                        expr_results.insert(*expr_name, false);
                    }
                }

                // Evaluate the final result expression and filter rows where it is true
                self.evaluate_result_expression(&expr_results, result_expression)
            })
            .collect();

        self.data = filtered_data;

        self
    }

    /// Prints the count of rows matching the filter criteria.
    pub fn print_count_where(
        &mut self,
        expressions: Vec<(&str, Exp)>,
        result_expression: &str,
    ) -> &mut Self {
        // Use the headers directly since we are not modifying data
        let headers = &self.headers;

        // Count the number of rows that match the filter
        let count = self
            .data
            .iter()
            .filter(|row| {
                let mut expr_results = HashMap::new();
                expr_results.insert("true", true);
                expr_results.insert("false", false);

                // Evaluate each expression
                for (expr_name, exp) in &expressions {
                    if let Some(column_index) = headers.iter().position(|h| h == &exp.column) {
                        if let Some(cell_value) = row.get(column_index) {
                            let result = match &exp.compare_with {
                                ExpVal::STR(value_str) => {
                                    value_str.apply(cell_value, &exp.operator, &exp.compare_as)
                                }
                                ExpVal::VEC(values) => {
                                    values.apply(cell_value, &exp.operator, &exp.compare_as)
                                }
                            };
                            //dbg!(&expr_name, &result);
                            expr_results.insert(&expr_name, result);
                        } else {
                            expr_results.insert(&expr_name, false);
                        }
                    } else {
                        println!("Column '{}' not found in headers.", exp.column);
                        expr_results.insert(*expr_name, false);
                    }
                }

                // Evaluate the final result expression
                self.evaluate_result_expression(&expr_results, result_expression)
            })
            .count();

        // Print the count
        println!("Count: {}", count);

        self
    }

    /// Sets a limit on the number of rows to be included in the CSV and truncates the data if it exceeds the limit.
    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);

        // Truncate the data vector if it exceeds the limit
        if self.data.len() > limit {
            self.data.truncate(limit);
        }

        self
    }

    pub fn limit_distributed_raw(&mut self, limit: usize) -> &mut Self {
        if limit >= self.data.len() || limit == 0 {
            self.limit = Some(self.data.len());
            return self;
        }

        let mut result = Vec::new();
        let step = self.data.len() as f64 / limit as f64;

        for i in 0..limit {
            let idx = (i as f64 * step).floor() as usize;
            result.push(self.data[idx].clone());
        }

        self.data = result;
        self.limit = Some(limit);
        self
    }

    pub fn limit_distributed_category(&mut self, limit: usize, column_name: &str) -> &mut Self {
        let column_index = match self.headers.iter().position(|r| r == column_name) {
            Some(index) => index,
            None => {
                self.error = Some(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Column name not found",
                )));
                return self;
            }
        };

        if limit >= self.data.len() || limit == 0 {
            self.limit = Some(self.data.len());
            return self;
        }

        let mut category_counts = std::collections::HashMap::new();
        for row in &self.data {
            *category_counts
                .entry(row[column_index].clone())
                .or_insert(0) += 1;
        }

        let mut category_limits = std::collections::HashMap::new();
        let total_categories = category_counts.len();
        let base_limit = limit / total_categories;
        let mut extra = limit % total_categories;

        for (category, _) in &category_counts {
            let mut this_limit = base_limit;
            if extra > 0 {
                this_limit += 1;
                extra -= 1;
            }
            category_limits.insert(category.clone(), this_limit);
        }

        let mut new_data = Vec::new();
        let mut selections = std::collections::HashMap::new();

        for row in &self.data {
            let category = &row[column_index];
            let count = selections.entry(category.clone()).or_insert(0);
            if *count < *category_limits.get(category).unwrap_or(&0) {
                new_data.push(row.clone());
                *count += 1;
            }
        }

        self.data = new_data;
        self.limit = Some(limit);
        self
    }

    pub fn limit_random(&mut self, limit: usize) -> &mut Self {
        if limit >= self.data.len() || limit == 0 {
            self.limit = Some(self.data.len());
            return self;
        }

        let mut rng = rand::thread_rng();
        let sample = self
            .data
            .as_slice()
            .choose_multiple(&mut rng, limit)
            .cloned()
            .collect();

        self.data = sample;
        self.limit = Some(limit);
        self
    }

    pub fn limit_where(
        &mut self,
        limit: usize,
        expressions: Vec<(&str, Exp)>,
        result_expression: &str,
        selection_strategy: &str,
    ) -> &mut Self {
        let headers_clone = self.headers.clone();
        let drained_data = self.data.drain(..).collect::<Vec<_>>();

        let mut filtered_data = Vec::new();
        let mut remaining_data = Vec::new();

        for row in drained_data {
            let mut expr_results = HashMap::new();
            expr_results.insert("true", true);
            expr_results.insert("false", false);

            for (expr_name, exp) in &expressions {
                if let Some(column_index) = headers_clone.iter().position(|h| h == &exp.column) {
                    if let Some(cell_value) = row.get(column_index) {
                        let result = match &exp.compare_with {
                            ExpVal::STR(value_str) => {
                                value_str.apply(cell_value, &exp.operator, &exp.compare_as)
                            }
                            ExpVal::VEC(values) => {
                                values.apply(cell_value, &exp.operator, &exp.compare_as)
                            }
                        };
                        expr_results.insert(*expr_name, result);
                    } else {
                        expr_results.insert(*expr_name, false);
                    }
                } else {
                    println!("Column '{}' not found in headers.", exp.column);
                    expr_results.insert(*expr_name, false);
                }
            }

            let result = self.evaluate_result_expression(&expr_results, result_expression);

            if result {
                filtered_data.push(row);
            } else {
                remaining_data.push(row);
            }
        }

        match selection_strategy {
            "TAKE:FIRST" => {
                self.data = filtered_data.into_iter().take(limit).collect();
            }
            "TAKE:LAST" => {
                self.data = filtered_data.into_iter().rev().take(limit).collect();
            }
            "TAKE:RANDOM" => {
                filtered_data.shuffle(&mut thread_rng());
                self.data = filtered_data.into_iter().take(limit).collect();
            }
            _ => {
                self.data = filtered_data.into_iter().take(limit).collect();
            }
        }

        self.data.extend(remaining_data);

        self
    }

    /// Helper function to clean a string value (removes surrounding quotes)
    fn clean_string_value(value: &str) -> String {
        value.trim_matches('\"').to_string()
    }

    /// Prints unique values for a specified column and returns self for chaining.
    pub fn print_unique(&mut self, column_name: &str) -> &mut Self {
        if let Some(index) = self.headers.iter().position(|h| h == column_name) {
            let mut unique_values: HashSet<String> = HashSet::new();
            for row in &self.data {
                if let Some(value) = row.get(index) {
                    unique_values.insert(Self::clean_string_value(value));
                }
            }
            print!("Unique values in '{}': ", column_name);
            for (i, value) in unique_values.iter().enumerate() {
                if i > 0 {
                    print!(", ");
                }
                print!("{}", value);
            }
            println!(); // Add a newline at the end
        } else {
            println!("Column '{}' not found", column_name);
        }
        self
    }

    pub fn print_unique_count(&mut self, column_name: &str) -> &mut Self {
        if let Some(index) = self.headers.iter().position(|h| h == column_name) {
            let mut unique_values: HashSet<String> = HashSet::new();
            for row in &self.data {
                if let Some(value) = row.get(index) {
                    unique_values.insert(Self::clean_string_value(value));
                }
            }
            println!(
                "Count of unique values in '{}': {}",
                column_name,
                unique_values.len()
            );
        } else {
            println!("Column '{}' not found", column_name);
        }
        self
    }

    /// Returns unique values for a specified column as a `Vec<String>`, with cleaner values.
    pub fn get_unique(&mut self, column_name: &str) -> Vec<String> {
        let mut unique_values: HashSet<String> = HashSet::new();
        if let Some(index) = self.headers.iter().position(|h| h == column_name) {
            for row in &self.data {
                if let Some(value) = row.get(index) {
                    unique_values.insert(Self::clean_string_value(value));
                }
            }
        }
        unique_values.into_iter().collect()
    }

    /// Returns the unique value for a specified column as a `String` if there's only one result. Prints a message if more than one value is detected, suggesting to use `get_unique()` instead.
    pub fn get(&mut self, column_name: &str) -> String {
        let mut unique_value: Option<String> = None;
        let mut multiple_values_detected = false;

        if let Some(index) = self.headers.iter().position(|h| h == column_name) {
            for row in &self.data {
                if let Some(value) = row.get(index) {
                    let cleaned_value = Self::clean_string_value(value);
                    if let Some(existing_value) = unique_value.take() {
                        if existing_value != cleaned_value {
                            multiple_values_detected = true;
                            break;
                        }
                    } else {
                        unique_value = Some(cleaned_value);
                    }
                }
            }
        }

        if multiple_values_detected {
            let message = "Multiple values detected. Use get_unique() instead".to_string();
            message
        } else {
            unique_value.unwrap_or_else(|| "No value found".to_string())
        }
    }

    /// Sets the value for a specified column in the single row of data and returns mutable reference to self.
    /// Adds the column and value if the column does not exist.
    pub fn set(&mut self, column_name: &str, value: &str) -> &mut Self {
        let column_index = self.headers.iter().position(|h| h == column_name);

        if let Some(index) = column_index {
            // Column exists, set its value in the data row
            if self.data.is_empty() {
                // If there's no row, add one
                let mut new_row = vec![String::new(); self.headers.len()];
                new_row[index] = value.to_string();
                self.data.push(new_row);
            } else {
                // Assuming only one row, set the value
                if let Some(row) = self.data.get_mut(0) {
                    row[index] = value.to_string();
                }
            }
        } else {
            // Column doesn't exist, add it and its value
            self.headers.push(column_name.to_string());
            if self.data.is_empty() {
                // If there's no row, add one with the value
                let new_row = vec![value.to_string()];
                self.data.push(new_row);
            } else {
                // Assuming only one row, add the value
                self.data
                    .iter_mut()
                    .for_each(|row| row.push(value.to_string()));
            }
        }
        self
    }

    /// Prints the frequency of each unique value in the specified columns of the CSV.
    /// This function first tries to sort values as numbers or timestamps. If they
    /// can't be parsed as such, it then sorts them in ascending alphabetical order.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of column names whose frequencies are to be printed.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    ///
    /// let mut builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string(), "event".to_string()],
    ///     vec![
    ///         vec!["2023-01-01".to_string(), "23.5".to_string(), "New Year".to_string()],
    ///         vec!["2023-01-01".to_string(), "23.5".to_string(), "Holiday".to_string()],
    ///         vec!["2023-01-02".to_string(), "24.1".to_string(), "Workday".to_string()],
    ///     ],
    /// );
    ///
    /// let frequencies = builder.get_freq(vec!["date", "event"]);
    ///
    /// let expected_date_freq = vec![
    ///     ("2023-01-01".to_string(), 2),
    ///     ("2023-01-02".to_string(), 1),
    /// ];
    /// let expected_event_freq = vec![
    ///     ("Holiday".to_string(), 1),
    ///     ("New Year".to_string(), 1),
    ///     ("Workday".to_string(), 1),
    /// ];
    ///
    /// assert_eq!(frequencies.get("date").unwrap(), &expected_date_freq);
    /// assert_eq!(frequencies.get("event").unwrap(), &expected_event_freq);
    /// ```
    pub fn get_freq(&mut self, columns: Vec<&str>) -> HashMap<String, Vec<(String, usize)>> {
        let mut results = HashMap::new();

        // Finding indices for each column
        let column_indices: Vec<usize> = columns
            .iter()
            .filter_map(|&col| self.headers.iter().position(|h| h == col))
            .collect();

        for &col_idx in &column_indices {
            let mut freq_map: HashMap<String, usize> = HashMap::new();
            for row in &self.data {
                if let Some(value) = row.get(col_idx) {
                    *freq_map.entry(value.clone()).or_insert(0) += 1;
                }
            }

            // Sorting the frequency map
            let mut sorted_freq: Vec<(String, usize)> = freq_map.into_iter().collect();
            sorted_freq.sort_by(|a, b| a.0.cmp(&b.0)); // Sorting alphabetically or by timestamp

            let column_name = &self.headers[col_idx];
            results.insert(column_name.clone(), sorted_freq);
        }

        results
    }

    /// Prints the frequency of each unique value in specified columns of the CSV, with optional grouping.
    /// This function first tries to sort values as numbers or timestamps. If they
    /// can't be parsed as such, it then sorts them in ascending alphabetical order.
    /// Groupings allow multiple values to be considered as one for the purpose of frequency calculation.
    ///
    /// # Arguments
    ///
    /// * `columns_with_groupings` - A vector of tuples where each tuple consists of a column name
    ///   and a vector of groupings. Each grouping is a tuple where the first element is a primary value
    ///   and the second element is a vector of values to be grouped under the primary value.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    ///
    /// let mut builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string(), "event".to_string()],
    ///     vec![
    ///         vec!["2023-01-01".to_string(), "23.5".to_string(), "New Year".to_string()],
    ///         vec!["2023-01-01".to_string(), "23.5".to_string(), "Holiday".to_string()],
    ///         vec!["2023-01-02".to_string(), "24.1".to_string(), "Workday".to_string()],
    ///     ],
    /// );
    ///
    /// // The following call to print_freq_mapped demonstrates how to group multiple values under a primary key.
    /// // For instance, both "New Year" and "Holiday" events could be grouped under "Holiday Season" for frequency counting.
    /// builder.print_freq_mapped(vec![
    ///     ("event", vec![
    ///         ("Holiday Season", vec!["New Year", "Holiday"]),
    ///         ("Regular Day", vec!["Workday"])
    ///     ])
    /// ]);
    /// // This will count "New Year" and "Holiday" as "Holiday Season", and "Workday" as "Regular Day".
    /// // Output should sort values based on their grouping and then alphabetically or numerically within each group.
    /// // Frequency for column 'event':
    /// //   Holiday Season: 2
    /// //   Regular Day: 1
    /// ```
    ///
    /// This method does not capture output in this example; it demonstrates the syntax for calling the function.
    pub fn get_freq_mapped(
        &mut self,
        columns_with_groupings: Vec<(&str, Vec<(&str, Vec<&str>)>)>,
    ) -> HashMap<String, Vec<(String, usize)>> {
        let mut results = HashMap::new();

        for (col, groupings) in columns_with_groupings {
            let col_idx = if let Some(index) = self.headers.iter().position(|r| r == col) {
                index
            } else {
                println!("Column '{}' not found.", col);
                continue;
            };

            let mut value_map: HashMap<String, String> = HashMap::new();
            let mut apply_groupings = true;

            for (primary_value, values) in groupings {
                if primary_value == "NO_GROUPINGS" {
                    apply_groupings = false;
                    break;
                }
                for value in values {
                    value_map.insert(value.to_string(), primary_value.to_string());
                }
            }

            let mut freq_map: HashMap<String, usize> = HashMap::new();

            // Count the frequency of each unique value
            for row in &self.data {
                if let Some(value) = row.get(col_idx) {
                    let value_str = value.to_string();
                    let grouped_value = if apply_groupings {
                        value_map.get(&value_str).unwrap_or(&value_str)
                    } else {
                        &value_str
                    };
                    *freq_map.entry(grouped_value.to_string()).or_insert(0) += 1;
                }
            }

            // Sorting the frequency map
            let mut sorted_freq: Vec<(String, usize)> = freq_map.into_iter().collect();
            sorted_freq.sort_by(|a, b| b.1.cmp(&a.1));

            results.insert(col.to_string(), sorted_freq);
        }

        results
    }

    /// Removes duplicate rows from the CSV data. This method ensures that only unique rows are retained in the `CsvBuilder`.
    pub fn remove_duplicates(&mut self) -> &mut Self {
        let original_count = self.data.len();
        let mut unique_rows = HashSet::new();
        self.data.retain(|row| unique_rows.insert(row.clone()));
        let duplicates_removed = original_count - unique_rows.len();

        println!("Number of duplicate rows removed: {}", duplicates_removed);

        self
    }

    /// Replaces multiple sets of string occurrences in specified data columns.
    pub fn replace_all(
        &mut self,
        columns: Vec<String>,
        replacements: Vec<(String, String)>,
    ) -> &mut Self {
        let apply_to_all = columns.iter().any(|col| col == "*");
        let column_indices: Vec<usize> = if apply_to_all {
            (0..self.headers.len()).collect()
        } else {
            columns
                .iter()
                .filter_map(|col| self.headers.iter().position(|h| h == col))
                .collect()
        };

        for row in &mut self.data {
            for &index in &column_indices {
                if let Some(item) = row.get_mut(index) {
                    for (from, to) in &replacements {
                        *item = item.replace(from, to);
                    }
                }
            }
        }
        self
    }

    /// Replaces empty string cells in specified data columns with a given replacement string.
    pub fn replace_all_empty_string_cells_with(
        &mut self,
        columns: Vec<&str>,
        replacement: &str,
    ) -> &mut Self {
        let apply_to_all = columns.iter().any(|&col| col == "*");
        let column_indices: Vec<usize> = if apply_to_all {
            (0..self.headers.len()).collect()
        } else {
            columns
                .iter()
                .filter_map(|&col| self.headers.iter().position(|h| h == col))
                .collect()
        };

        for row in &mut self.data {
            for &index in &column_indices {
                if let Some(item) = row.get_mut(index) {
                    if item.is_empty() {
                        *item = replacement.to_string();
                    }
                }
            }
        }
        self
    }

    /// Trims white spaces at the beginning and end of all cells in all columns.
    pub fn trim_all(&mut self) -> &mut Self {
        for row in &mut self.data {
            for item in row.iter_mut() {
                *item = item.trim().to_string();
            }
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_file(
    ///     &csv_path_b_str,
    ///     "LEFT_JOIN",
    ///     "id",
    ///     "id",
    ///     "75"
    ///     ).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn union_with_csv_file(
        &mut self,
        file_b_path: &str,
        join_type: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
        size_threshold: &str,
    ) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.union_with_csv_file_big(
                file_b_path,
                DaskJoinerConfig {
                    join_type: join_type.to_string(),
                    table_a_ref_column: table_a_ref_column.to_string(),
                    table_b_ref_column: table_b_ref_column.to_string(),
                },
            )
            .await
        } else {
            self.union_with_csv_file_small(
                file_b_path,
                join_type,
                table_a_ref_column,
                table_b_ref_column,
            )
            .await
        }
    }

    /// Union with another CsvBuilder and handle based on the size of the CsvBuilder object.

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_builder(
    ///     csv_b_builder,
    ///     "LEFT_JOIN",
    ///     "id",
    ///     "id",
    ///     "75"
    ///     ).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```
    pub async fn union_with_csv_builder(
        &mut self,
        file_b_builder: CsvBuilder,
        join_type: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
        size_threshold: &str,
    ) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        //dbg!(&size);
        if size > size_threshold_bytes {
            self.union_with_csv_builder_big(
                file_b_builder,
                DaskJoinerConfig {
                    join_type: join_type.to_string(),
                    table_a_ref_column: table_a_ref_column.to_string(),
                    table_b_ref_column: table_b_ref_column.to_string(),
                },
            )
            .await
        } else {
            self.union_with_csv_builder_small(
                file_b_builder,
                join_type,
                table_a_ref_column,
                table_b_ref_column,
            )
            .await
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_file_small(
    ///     &csv_path_b_str,
    ///     "LEFT_JOIN",
    ///     "id",
    ///     "id",
    ///     ).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/union_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/union_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_file_small(
    ///     &csv_path_b_str,
    ///     "UNION",
    ///     "",
    ///     "",
    ///     ).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/union_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/union_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_file_small(
    ///     &csv_path_b_str,
    ///     "BAG_UNION",
    ///     "",
    ///     "",
    ///     ).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn union_with_csv_file_small(
        &mut self,
        file_b_path: &str,
        union_type: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
    ) -> &mut Self {
        let join_columns = vec![table_a_ref_column, table_b_ref_column];
        let mut temp_builder = Self::from_csv(file_b_path);

        // Check for loading errors
        if temp_builder.error.is_some() {
            self.error = temp_builder.error;
            return self;
        }

        match union_type {
            "UNION" => {
                let mut unique_signatures = HashSet::new();
                let mut combined_data = Vec::new();

                let compute_signature = |row: &[String]| row.join(",");

                // Process the current CsvBuilder's data
                for row in &self.data {
                    let signature = compute_signature(row);
                    //dbg!(&signature);
                    if unique_signatures.insert(signature.clone()) {
                        combined_data.push(row.clone());
                    }
                }

                // Process the incoming CsvBuilder's data
                for row in &temp_builder.data {
                    let signature = compute_signature(row);
                    //dbg!(&signature);
                    if unique_signatures.insert(signature.clone()) {
                        let mut aligned_row = vec![String::new(); self.headers.len()];
                        for (i, header) in self.headers.iter().enumerate() {
                            if let Some(pos) = temp_builder.headers.iter().position(|h| h == header)
                            {
                                aligned_row[i] = row[pos].clone();
                            } else {
                                aligned_row[i] = "".to_string(); // Fill missing headers with empty string
                            }
                        }
                        combined_data.push(aligned_row);
                    }
                }

                // Update the builder's data with the combined data
                self.data = combined_data;
            }

            "BAG_UNION" => {
                let headers_set: HashSet<_> = self.headers.iter().collect();

                // Identify unique headers using parallel processing
                let unique_headers_temp: Vec<String> = temp_builder
                    .headers
                    .par_iter()
                    .filter_map(|h| {
                        if headers_set.contains(h) {
                            None
                        } else {
                            Some(h.clone())
                        }
                    })
                    .collect();

                // Update the headers of the current builder
                self.headers.extend(unique_headers_temp);

                // Append data from temp_builder to self using parallel processing
                self.data.par_extend(temp_builder.data.par_iter().cloned());
            }

            "LEFT_JOIN" => {
                let join_columns_valid = join_columns.iter().all(|&col| {
                    self.headers.iter().any(|h| h.as_str() == col)
                        && temp_builder.headers.iter().any(|h| h.as_str() == col)
                });

                if !join_columns_valid {
                    self.error = Some(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Join columns must exist in both datasets",
                    )));
                    return self;
                }

                // Add "joined_" prefix to each header in temp_builder that is not a join column
                for header in temp_builder.headers.iter_mut() {
                    if !join_columns.contains(&header.as_str()) {
                        *header = format!("joined_{}", header);
                    }
                }

                // Combine headers from both datasets
                let mut all_headers = self.headers.clone();
                for header in &temp_builder.headers {
                    if !all_headers.contains(header) {
                        all_headers.push(header.clone());
                    }
                }
                self.headers = all_headers;

                let temp_header_to_index: HashMap<_, _> = temp_builder
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(i, header)| (header.clone(), i))
                    .collect();

                let header_to_index: HashMap<String, usize> = self
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(index, header)| (header.clone(), index))
                    .collect();

                // Precompute keys for the temp_builder data with corresponding rows stored
                let temp_keys_map: HashMap<Vec<&str>, &Vec<String>> = temp_builder
                    .data
                    .iter()
                    .map(|temp_row| {
                        let key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| {
                                let index = *temp_header_to_index
                                    .get(col)
                                    .expect("Column not found in temp_header_to_index");
                                temp_row[index].as_str()
                            })
                            .collect();
                        (key, temp_row)
                    })
                    .collect();

                let result_data: Vec<Vec<String>> = self
                    .data
                    .par_iter()
                    .map(|self_row| {
                        let mut combined_row = vec![String::new(); self.headers.len()];

                        // Copy data from the current row
                        for (i, value) in self_row.iter().enumerate() {
                            combined_row[i] = value.clone();
                        }

                        // Generate self_key using the hashmap for quick index retrieval
                        let self_key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| self_row[header_to_index[&*col]].as_str())
                            .collect();

                        // Lookup in the precomputed hashmap
                        if let Some(temp_row) = temp_keys_map.get(&self_key) {
                            for (header, &index) in temp_header_to_index.iter() {
                                if !join_columns.contains(&header.as_str()) {
                                    if let Some(&pos) = header_to_index.get(header) {
                                        combined_row[pos] = temp_row[index].clone();
                                    }
                                }
                            }
                        }

                        combined_row // Return the combined row to be included in the result_data
                    })
                    .collect();

                // Update the builder's data with the result of the join operation
                self.data = result_data;
            }

            "RIGHT_JOIN" => {
                mem::swap(self, &mut temp_builder);
                // Validate join columns exist in both datasets
                let join_columns_valid = join_columns.iter().all(|&col| {
                    self.headers.iter().any(|h| h.as_str() == col)
                        && temp_builder.headers.iter().any(|h| h.as_str() == col)
                });

                if !join_columns_valid {
                    self.error = Some(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Join columns must exist in both datasets",
                    )));
                    return self;
                }

                // Add "joined_" prefix to each header in temp_builder that is not a join column
                for header in temp_builder.headers.iter_mut() {
                    if !join_columns.contains(&header.as_str()) {
                        *header = format!("joined_{}", header);
                    }
                }

                // Combine headers from both datasets
                let mut all_headers = self.headers.clone();
                for header in &temp_builder.headers {
                    if !all_headers.contains(header) {
                        all_headers.push(header.clone());
                    }
                }
                self.headers = all_headers;

                let temp_header_to_index: HashMap<_, _> = temp_builder
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(i, header)| (header.clone(), i))
                    .collect();

                let header_to_index: HashMap<String, usize> = self
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(index, header)| (header.clone(), index))
                    .collect();

                // Precompute keys for the temp_builder data with corresponding rows stored
                let temp_keys_map: HashMap<Vec<&str>, &Vec<String>> = temp_builder
                    .data
                    .iter()
                    .map(|temp_row| {
                        let key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| {
                                let index = *temp_header_to_index
                                    .get(col)
                                    .expect("Column not found in temp_header_to_index");
                                temp_row[index].as_str()
                            })
                            .collect();
                        (key, temp_row)
                    })
                    .collect();

                let result_data: Vec<Vec<String>> = self
                    .data
                    .par_iter()
                    .map(|self_row| {
                        let mut combined_row = vec![String::new(); self.headers.len()];

                        // Copy data from the current row
                        for (i, value) in self_row.iter().enumerate() {
                            combined_row[i] = value.clone();
                        }

                        // Generate self_key using the hashmap for quick index retrieval
                        let self_key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| self_row[header_to_index[&*col]].as_str())
                            .collect();

                        // Lookup in the precomputed hashmap
                        if let Some(temp_row) = temp_keys_map.get(&self_key) {
                            for (header, &index) in temp_header_to_index.iter() {
                                if !join_columns.contains(&header.as_str()) {
                                    if let Some(&pos) = header_to_index.get(header) {
                                        combined_row[pos] = temp_row[index].clone();
                                    }
                                }
                            }
                        }

                        combined_row // Return the combined row to be included in the result_data
                    })
                    .collect();

                // Update the builder's data with the result of the join operation
                self.data = result_data;
            }

            "OUTER_FULL_JOIN" => {
                let join_columns_valid = join_columns.iter().all(|&col| {
                    self.headers.iter().any(|h| h.as_str() == col)
                        && temp_builder.headers.iter().any(|h| h.as_str() == col)
                });

                if !join_columns_valid {
                    self.error = Some(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Join columns must exist in both datasets",
                    )));
                    return self;
                }

                for header in temp_builder.headers.iter_mut() {
                    if !join_columns.contains(&header.as_str()) {
                        *header = format!("joined_{}", header);
                    }
                }

                //dbg!(&temp_builder.headers);
                let mut all_headers = self.headers.clone();
                for header in &temp_builder.headers {
                    if !all_headers.contains(header) {
                        all_headers.push(header.clone());
                    }
                }
                self.headers = all_headers;

                //dbg!(&self.headers);

                let mut result_data = vec![];

                let temp_header_to_index: HashMap<_, _> = temp_builder
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(i, header)| (header.clone(), i))
                    .collect();

                // Initialize a set to track which rows from temp_builder have been matched.
                let mut matched_temp_rows = std::collections::HashSet::new();

                for self_row in &self.data {
                    let mut combined_row = vec![String::new(); self.headers.len()];

                    // Populate combined_row with values from self_row.
                    for (i, value) in self_row.iter().enumerate() {
                        combined_row[i] = value.clone();
                    }

                    let mut row_found = false;

                    for (temp_row_index, temp_row) in temp_builder.data.iter().enumerate() {
                        let self_key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| {
                                let index = self.headers.iter().position(|h| h == col).unwrap();
                                self_row[index].as_str()
                            })
                            .collect();

                        let temp_key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| {
                                let index = *temp_header_to_index
                                    .get(col)
                                    .expect("Column not found in temp_header_to_index");
                                temp_row[index].as_str()
                            })
                            .collect();

                        if self_key == temp_key {
                            matched_temp_rows.insert(temp_row_index); // Mark this temp row as matched.
                            for (header, &index) in &temp_header_to_index {
                                if !join_columns.contains(&header.as_str()) {
                                    let pos =
                                        self.headers.iter().position(|h| h == header).unwrap();
                                    combined_row[pos] = temp_row[index].clone();
                                }
                            }
                            row_found = true;
                            break; // Matching row found, no need to search further.
                        }
                    }

                    if !row_found {
                        // Extend combined_row with empty strings for alignment.
                        for i in self_row.len()..self.headers.len() {
                            combined_row[i] = "".to_string();
                        }
                    }

                    //dbg!(&combined_row);

                    result_data.push(combined_row);
                }

                // Include rows from temp_builder that were not matched
                for (index, temp_row) in temp_builder.data.iter().enumerate() {
                    if !matched_temp_rows.contains(&index) {
                        let mut combined_row = vec![String::new(); self.headers.len()];
                        for (header, &temp_index) in &temp_header_to_index {
                            //if !join_columns.contains(&header.as_str()) {
                            let pos = self.headers.iter().position(|h| h == header).unwrap();
                            combined_row[pos] = temp_row[temp_index].clone();
                            //}
                        }
                        result_data.push(combined_row);
                    }
                }

                // Update the data with the result from the full outer join.
                self.data = result_data;
            }

            _ => {
                println!("Unsupported union operation type: {}", union_type);
            }
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_builder_small(
    ///     csv_b_builder,
    ///     "LEFT_JOIN",
    ///     "id",
    ///     "id",
    ///     ).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```
    ///

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/union_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/union_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_builder_small(
    ///     csv_b_builder,
    ///     "UNION",
    ///     "",
    ///     "",
    ///     ).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```
    ///

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/union_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/union_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_builder_small(
    ///     csv_b_builder,
    ///     "BAG_UNION",
    ///     "",
    ///     "",
    ///     ).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```
    ///

    pub async fn union_with_csv_builder_small(
        &mut self,
        file_b_builder: CsvBuilder,
        union_type: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
    ) -> &mut Self {
        let join_columns = vec![table_a_ref_column, table_b_ref_column];
        let mut temp_builder = Self::from_copy(&file_b_builder);

        // Check for loading errors
        if temp_builder.error.is_some() {
            self.error = temp_builder.error;
            return self;
        }

        //dbg!(&union_type, &self, &temp_builder);

        match union_type {
            "UNION" => {
                let mut unique_signatures = HashSet::new();
                let mut combined_data = Vec::new();

                let compute_signature = |row: &[String]| row.join(",");

                // Process the current CsvBuilder's data
                for row in &self.data {
                    let signature = compute_signature(row);
                    //dbg!(&signature);
                    if unique_signatures.insert(signature.clone()) {
                        combined_data.push(row.clone());
                    }
                }

                // Process the incoming CsvBuilder's data
                for row in &temp_builder.data {
                    let signature = compute_signature(row);
                    //dbg!(&signature);
                    if unique_signatures.insert(signature.clone()) {
                        let mut aligned_row = vec![String::new(); self.headers.len()];
                        for (i, header) in self.headers.iter().enumerate() {
                            if let Some(pos) = temp_builder.headers.iter().position(|h| h == header)
                            {
                                aligned_row[i] = row[pos].clone();
                            } else {
                                aligned_row[i] = "".to_string(); // Fill missing headers with empty string
                            }
                        }
                        combined_data.push(aligned_row);
                    }
                }

                //dbg!(&combined_data);
                // Update the builder's data with the combined data
                self.data = combined_data;
            }

            "BAG_UNION" => {
                let headers_set: HashSet<_> = self.headers.iter().collect();

                // Identify unique headers using parallel processing
                let unique_headers_temp: Vec<String> = temp_builder
                    .headers
                    .par_iter()
                    .filter_map(|h| {
                        if headers_set.contains(h) {
                            None
                        } else {
                            Some(h.clone())
                        }
                    })
                    .collect();

                // Update the headers of the current builder
                self.headers.extend(unique_headers_temp);

                // Append data from temp_builder to self using parallel processing
                self.data.par_extend(temp_builder.data.par_iter().cloned());
            }

            "LEFT_JOIN" => {
                // Validate join columns exist in both datasets
                let join_columns_valid = join_columns.iter().all(|&col| {
                    self.headers.iter().any(|h| h.as_str() == col)
                        && temp_builder.headers.iter().any(|h| h.as_str() == col)
                });

                if !join_columns_valid {
                    self.error = Some(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Join columns must exist in both datasets",
                    )));
                    return self;
                }

                // Add "joined_" prefix to each header in temp_builder that is not a join column
                for header in temp_builder.headers.iter_mut() {
                    if !join_columns.contains(&header.as_str()) {
                        *header = format!("joined_{}", header);
                    }
                }

                // Combine headers from both datasets
                let mut all_headers = self.headers.clone();
                for header in &temp_builder.headers {
                    if !all_headers.contains(header) {
                        all_headers.push(header.clone());
                    }
                }
                self.headers = all_headers;

                let temp_header_to_index: HashMap<_, _> = temp_builder
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(i, header)| (header.clone(), i))
                    .collect();

                let header_to_index: HashMap<String, usize> = self
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(index, header)| (header.clone(), index))
                    .collect();

                // Precompute keys for the temp_builder data with corresponding rows stored
                let temp_keys_map: HashMap<Vec<&str>, &Vec<String>> = temp_builder
                    .data
                    .iter()
                    .map(|temp_row| {
                        let key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| {
                                let index = *temp_header_to_index
                                    .get(col)
                                    .expect("Column not found in temp_header_to_index");
                                temp_row[index].as_str()
                            })
                            .collect();
                        (key, temp_row)
                    })
                    .collect();

                let result_data: Vec<Vec<String>> = self
                    .data
                    .par_iter()
                    .map(|self_row| {
                        let mut combined_row = vec![String::new(); self.headers.len()];

                        // Copy data from the current row
                        for (i, value) in self_row.iter().enumerate() {
                            combined_row[i] = value.clone();
                        }

                        // Generate self_key using the hashmap for quick index retrieval
                        let self_key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| self_row[header_to_index[&*col]].as_str())
                            .collect();

                        // Lookup in the precomputed hashmap
                        if let Some(temp_row) = temp_keys_map.get(&self_key) {
                            for (header, &index) in temp_header_to_index.iter() {
                                if !join_columns.contains(&header.as_str()) {
                                    if let Some(&pos) = header_to_index.get(header) {
                                        combined_row[pos] = temp_row[index].clone();
                                    }
                                }
                            }
                        }

                        combined_row // Return the combined row to be included in the result_data
                    })
                    .collect();

                // Update the builder's data with the result of the join operation
                self.data = result_data;
            }

            "RIGHT_JOIN" => {
                mem::swap(self, &mut temp_builder);
                // Validate join columns exist in both datasets
                let join_columns_valid = join_columns.iter().all(|&col| {
                    self.headers.iter().any(|h| h.as_str() == col)
                        && temp_builder.headers.iter().any(|h| h.as_str() == col)
                });

                if !join_columns_valid {
                    self.error = Some(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Join columns must exist in both datasets",
                    )));
                    return self;
                }

                // Add "joined_" prefix to each header in temp_builder that is not a join column
                for header in temp_builder.headers.iter_mut() {
                    if !join_columns.contains(&header.as_str()) {
                        *header = format!("joined_{}", header);
                    }
                }

                // Combine headers from both datasets
                let mut all_headers = self.headers.clone();
                for header in &temp_builder.headers {
                    if !all_headers.contains(header) {
                        all_headers.push(header.clone());
                    }
                }
                self.headers = all_headers;

                let temp_header_to_index: HashMap<_, _> = temp_builder
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(i, header)| (header.clone(), i))
                    .collect();

                let header_to_index: HashMap<String, usize> = self
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(index, header)| (header.clone(), index))
                    .collect();

                // Precompute keys for the temp_builder data with corresponding rows stored
                let temp_keys_map: HashMap<Vec<&str>, &Vec<String>> = temp_builder
                    .data
                    .iter()
                    .map(|temp_row| {
                        let key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| {
                                let index = *temp_header_to_index
                                    .get(col)
                                    .expect("Column not found in temp_header_to_index");
                                temp_row[index].as_str()
                            })
                            .collect();
                        (key, temp_row)
                    })
                    .collect();

                let result_data: Vec<Vec<String>> = self
                    .data
                    .par_iter()
                    .map(|self_row| {
                        let mut combined_row = vec![String::new(); self.headers.len()];

                        // Copy data from the current row
                        for (i, value) in self_row.iter().enumerate() {
                            combined_row[i] = value.clone();
                        }

                        // Generate self_key using the hashmap for quick index retrieval
                        let self_key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| self_row[header_to_index[&*col]].as_str())
                            .collect();

                        // Lookup in the precomputed hashmap
                        if let Some(temp_row) = temp_keys_map.get(&self_key) {
                            for (header, &index) in temp_header_to_index.iter() {
                                if !join_columns.contains(&header.as_str()) {
                                    if let Some(&pos) = header_to_index.get(header) {
                                        combined_row[pos] = temp_row[index].clone();
                                    }
                                }
                            }
                        }

                        combined_row // Return the combined row to be included in the result_data
                    })
                    .collect();

                // Update the builder's data with the result of the join operation
                self.data = result_data;
            }

            "OUTER_FULL_JOIN" => {
                let join_columns_valid = join_columns.iter().all(|&col| {
                    self.headers.iter().any(|h| h.as_str() == col)
                        && temp_builder.headers.iter().any(|h| h.as_str() == col)
                });

                if !join_columns_valid {
                    self.error = Some(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Join columns must exist in both datasets",
                    )));
                    return self;
                }

                for header in temp_builder.headers.iter_mut() {
                    if !join_columns.contains(&header.as_str()) {
                        *header = format!("joined_{}", header);
                    }
                }

                //dbg!(&temp_builder.headers);
                let mut all_headers = self.headers.clone();
                for header in &temp_builder.headers {
                    if !all_headers.contains(header) {
                        all_headers.push(header.clone());
                    }
                }
                self.headers = all_headers;

                //dbg!(&self.headers);

                let mut result_data = vec![];

                let temp_header_to_index: HashMap<_, _> = temp_builder
                    .headers
                    .iter()
                    .enumerate()
                    .map(|(i, header)| (header.clone(), i))
                    .collect();

                // Initialize a set to track which rows from temp_builder have been matched.
                let mut matched_temp_rows = std::collections::HashSet::new();

                for self_row in &self.data {
                    let mut combined_row = vec![String::new(); self.headers.len()];

                    // Populate combined_row with values from self_row.
                    for (i, value) in self_row.iter().enumerate() {
                        combined_row[i] = value.clone();
                    }

                    let mut row_found = false;

                    for (temp_row_index, temp_row) in temp_builder.data.iter().enumerate() {
                        let self_key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| {
                                let index = self.headers.iter().position(|h| h == col).unwrap();
                                self_row[index].as_str()
                            })
                            .collect();

                        let temp_key: Vec<&str> = join_columns
                            .iter()
                            .map(|&col| {
                                let index = *temp_header_to_index
                                    .get(col)
                                    .expect("Column not found in temp_header_to_index");
                                temp_row[index].as_str()
                            })
                            .collect();

                        if self_key == temp_key {
                            matched_temp_rows.insert(temp_row_index); // Mark this temp row as matched.
                            for (header, &index) in &temp_header_to_index {
                                if !join_columns.contains(&header.as_str()) {
                                    let pos =
                                        self.headers.iter().position(|h| h == header).unwrap();
                                    combined_row[pos] = temp_row[index].clone();
                                }
                            }
                            row_found = true;
                            break; // Matching row found, no need to search further.
                        }
                    }

                    if !row_found {
                        // Extend combined_row with empty strings for alignment.
                        for i in self_row.len()..self.headers.len() {
                            combined_row[i] = "".to_string();
                        }
                    }

                    //dbg!(&combined_row);

                    result_data.push(combined_row);
                }

                // Include rows from temp_builder that were not matched
                for (index, temp_row) in temp_builder.data.iter().enumerate() {
                    if !matched_temp_rows.contains(&index) {
                        let mut combined_row = vec![String::new(); self.headers.len()];
                        for (header, &temp_index) in &temp_header_to_index {
                            //if !join_columns.contains(&header.as_str()) {
                            let pos = self.headers.iter().position(|h| h == header).unwrap();
                            combined_row[pos] = temp_row[temp_index].clone();
                            //}
                        }
                        result_data.push(combined_row);
                    }
                }

                // Update the data with the result from the full outer join.
                self.data = result_data;
            }

            _ => {
                println!("Unsupported union operation type: {}", union_type);
            }
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskJoinerConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    ///
    /// let dask_joiner_config = DaskJoinerConfig {
    ///     join_type: "LEFT_JOIN".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_file_big(&csv_path_b_str, dask_joiner_config).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn union_with_csv_file_big(
        &mut self,
        file_b_path: &str,
        dask_joiner_config: DaskJoinerConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.from_copy();
        let temp_builder_b = CsvBuilder::from_csv(file_b_path);

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let _ = writeln!(temp_file_a, "{}", temp_builder_a.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    if row.len() == header_len_a {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        //println!("Skipping malformed row: {:?}", row);
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !temp_builder_b.headers.is_empty() {
                    let _ = writeln!(temp_file_b, "{}", temp_builder_b.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_b = temp_builder_b.headers.len();
                for row in &temp_builder_b.data {
                    if row.len() == header_len_b {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        //println!("Skipping malformed row: {:?}", row);
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_joiner(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_joiner_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskJoinerConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    ///
    /// let dask_joiner_config = DaskJoinerConfig {
    ///     join_type: "LEFT_JOIN".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.union_with_csv_builder_big(csv_b_builder, dask_joiner_config).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn union_with_csv_builder_big(
        &mut self,
        file_b_builder: CsvBuilder,
        dask_joiner_config: DaskJoinerConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.from_copy();

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let _ = writeln!(temp_file_a, "{}", temp_builder_a.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    if row.len() == header_len_a {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !file_b_builder.headers.is_empty() {
                    let _ = writeln!(temp_file_b, "{}", file_b_builder.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_b = file_b_builder.headers.len();
                for row in &file_b_builder.data {
                    if row.len() == header_len_b {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_joiner(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_joiner_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskIntersectorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.intersection_with_csv_file(&csv_path_b_str, "id", "id", "75").await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn intersection_with_csv_file(
        &mut self,
        file_b_path: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
        size_threshold: &str,
    ) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.intersection_with_csv_file_big(
                file_b_path,
                DaskIntersectorConfig {
                    table_a_ref_column: table_a_ref_column.to_string(),
                    table_b_ref_column: table_b_ref_column.to_string(),
                },
            )
            .await
        } else {
            self.intersection_with_csv_file_small(
                file_b_path,
                table_a_ref_column,
                table_b_ref_column,
            )
            .await
        }
    }

    /// ```     
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.intersection_with_csv_builder(csv_b_builder, "id", "id", "75").await;
    ///         
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });     
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn intersection_with_csv_builder(
        &mut self,
        file_b_builder: CsvBuilder,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
        size_threshold: &str,
    ) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.intersection_with_csv_builder_big(
                file_b_builder,
                DaskIntersectorConfig {
                    table_a_ref_column: table_a_ref_column.to_string(),
                    table_b_ref_column: table_b_ref_column.to_string(),
                },
            )
            .await
        } else {
            self.intersection_with_csv_builder_small(
                file_b_builder,
                table_a_ref_column,
                table_b_ref_column,
            )
            .await
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskIntersectorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.intersection_with_csv_file_small(&csv_path_b_str, "id", "id").await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn intersection_with_csv_file_small(
        &mut self,
        file_b_path: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
    ) -> &mut Self {
        let key_columns = vec![table_a_ref_column, table_b_ref_column];
        let temp_builder = Self::from_csv(file_b_path);

        // Check for loading errors
        if temp_builder.error.is_some() {
            self.error = temp_builder.error;
            return self;
        }

        let key_columns_set: HashSet<&str> = key_columns.iter().copied().collect();

        if !key_columns_set.iter().all(|&k| {
            self.headers.contains(&k.to_string()) && temp_builder.headers.contains(&k.to_string())
        }) {
            self.error = Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Key columns must exist in both datasets",
            )));
            return self;
        }

        let self_key_indices: Vec<usize> = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if key_columns_set.contains(header.as_str()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let temp_key_indices: Vec<usize> = temp_builder
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if key_columns_set.contains(header.as_str()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let mut combined_data = Vec::new();

        for self_row in &self.data {
            // Get key values from self_row for comparison, used to find matching rows
            let self_key_tuple: Vec<String> = self_key_indices
                .iter()
                .map(|&i| self_row[i].clone())
                .collect();

            for temp_row in &temp_builder.data {
                // Get key values from temp_row for comparison
                let temp_key_tuple: Vec<String> = temp_key_indices
                    .iter()
                    .map(|&i| temp_row[i].clone())
                    .collect();

                // If the key values match, combine the rows
                if self_key_tuple == temp_key_tuple {
                    // Combine the rows without duplicating the key values
                    // Key values from self_row are included first
                    let mut combined_row: Vec<String> = Vec::new();

                    // Add key values from self_row
                    for &index in &self_key_indices {
                        combined_row.push(self_row[index].clone());
                    }

                    // Add non-key values from self_row
                    for (i, value) in self_row.iter().enumerate() {
                        if !self_key_indices.contains(&i) {
                            combined_row.push(value.clone());
                        }
                    }

                    // Add non-key values from temp_row, excluding temp_row's key values to prevent duplication
                    for (i, value) in temp_row.iter().enumerate() {
                        if !temp_key_indices.contains(&i) {
                            combined_row.push(value.clone());
                        }
                    }

                    combined_data.push(combined_row);
                }
            }
        }

        // Assign the combined data to self.data
        self.data = combined_data;

        // Update headers logic remains the same, correctly placing key columns first, followed by non-key columns
        let key_headers: Vec<String> = self_key_indices
            .iter()
            .map(|&i| self.headers[i].clone())
            .collect();

        let non_key_self_headers: Vec<String> = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if !self_key_indices.contains(&i) {
                    Some(header.clone())
                } else {
                    None
                }
            })
            .collect();

        let non_key_temp_headers: Vec<String> = temp_builder
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if !temp_key_indices.contains(&i) {
                    Some(header.clone())
                } else {
                    None
                }
            })
            .collect();

        // Combine key headers with non-key headers from both self and temp_builder
        let combined_headers: Vec<String> = key_headers
            .into_iter()
            .chain(non_key_self_headers.into_iter())
            .chain(non_key_temp_headers.into_iter())
            .collect();

        // Update self.headers with the combined headers
        self.headers = combined_headers;

        self
    }

    /// ```     
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///         
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///         
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///                 
    /// let result = csv_a_builder.intersection_with_csv_builder_small(csv_b_builder, "id", "id").await;
    ///         
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });     
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn intersection_with_csv_builder_small(
        &mut self,
        file_b_builder: CsvBuilder,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
    ) -> &mut Self {
        let key_columns = vec![table_a_ref_column, table_b_ref_column];
        let temp_builder = Self::from_copy(&file_b_builder);

        // Check for loading errors
        if temp_builder.error.is_some() {
            self.error = temp_builder.error;
            return self;
        }

        let key_columns_set: HashSet<&str> = key_columns.iter().copied().collect();

        if !key_columns_set.iter().all(|&k| {
            self.headers.contains(&k.to_string()) && temp_builder.headers.contains(&k.to_string())
        }) {
            self.error = Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Key columns must exist in both datasets",
            )));
            return self;
        }

        let self_key_indices: Vec<usize> = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if key_columns_set.contains(header.as_str()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let temp_key_indices: Vec<usize> = temp_builder
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if key_columns_set.contains(header.as_str()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let mut combined_data = Vec::new();

        for self_row in &self.data {
            // Get key values from self_row for comparison, used to find matching rows
            let self_key_tuple: Vec<String> = self_key_indices
                .iter()
                .map(|&i| self_row[i].clone())
                .collect();

            for temp_row in &temp_builder.data {
                // Get key values from temp_row for comparison
                let temp_key_tuple: Vec<String> = temp_key_indices
                    .iter()
                    .map(|&i| temp_row[i].clone())
                    .collect();

                // If the key values match, combine the rows
                if self_key_tuple == temp_key_tuple {
                    // Combine the rows without duplicating the key values
                    // Key values from self_row are included first
                    let mut combined_row: Vec<String> = Vec::new();

                    // Add key values from self_row
                    for &index in &self_key_indices {
                        combined_row.push(self_row[index].clone());
                    }

                    // Add non-key values from self_row
                    for (i, value) in self_row.iter().enumerate() {
                        if !self_key_indices.contains(&i) {
                            combined_row.push(value.clone());
                        }
                    }

                    // Add non-key values from temp_row, excluding temp_row's key values to prevent duplication
                    for (i, value) in temp_row.iter().enumerate() {
                        if !temp_key_indices.contains(&i) {
                            combined_row.push(value.clone());
                        }
                    }

                    combined_data.push(combined_row);
                }
            }
        }

        // Assign the combined data to self.data
        self.data = combined_data;

        // Update headers logic remains the same, correctly placing key columns first, followed by non-key columns
        let key_headers: Vec<String> = self_key_indices
            .iter()
            .map(|&i| self.headers[i].clone())
            .collect();

        let non_key_self_headers: Vec<String> = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if !self_key_indices.contains(&i) {
                    Some(header.clone())
                } else {
                    None
                }
            })
            .collect();

        let non_key_temp_headers: Vec<String> = temp_builder
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if !temp_key_indices.contains(&i) {
                    Some(header.clone())
                } else {
                    None
                }
            })
            .collect();

        // Combine key headers with non-key headers from both self and temp_builder
        let combined_headers: Vec<String> = key_headers
            .into_iter()
            .chain(non_key_self_headers.into_iter())
            .chain(non_key_temp_headers.into_iter())
            .collect();

        // Update self.headers with the combined headers
        self.headers = combined_headers;

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskIntersectorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    ///
    /// let dask_intersector_config = DaskIntersectorConfig {
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.intersection_with_csv_file_big(&csv_path_b_str, dask_intersector_config).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn intersection_with_csv_file_big(
        &mut self,
        file_b_path: &str,
        dask_intersector_config: DaskIntersectorConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.from_copy();
        let temp_builder_b = CsvBuilder::from_csv(file_b_path);

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let _ = writeln!(temp_file_a, "{}", temp_builder_a.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    if row.len() == header_len_a {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        //println!("Skipping malformed row: {:?}", row);
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !temp_builder_b.headers.is_empty() {
                    let _ = writeln!(temp_file_b, "{}", temp_builder_b.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_b = temp_builder_b.headers.len();
                for row in &temp_builder_b.data {
                    if row.len() == header_len_b {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        //println!("Skipping malformed row: {:?}", row);
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_intersector(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_intersector_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskIntersectorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    ///
    /// let dask_intersector_config = DaskIntersectorConfig {
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.intersection_with_csv_builder_big(csv_b_builder, dask_intersector_config).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn intersection_with_csv_builder_big(
        &mut self,
        file_b_builder: CsvBuilder,
        dask_intersector_config: DaskIntersectorConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.from_copy();

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let _ = writeln!(temp_file_a, "{}", temp_builder_a.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    if row.len() == header_len_a {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !file_b_builder.headers.is_empty() {
                    let _ = writeln!(temp_file_b, "{}", file_b_builder.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_b = file_b_builder.headers.len();
                for row in &file_b_builder.data {
                    if row.len() == header_len_b {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_intersector(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_intersector_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.difference_with_csv_file(&csv_path_b_str, "NORMAL", "id", "id", "75").await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn difference_with_csv_file(
        &mut self,
        file_b_path: &str,
        difference_type: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
        size_threshold: &str,
    ) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.difference_with_csv_file_big(
                file_b_path,
                DaskDifferentiatorConfig {
                    difference_type: difference_type.to_string(),
                    table_a_ref_column: table_a_ref_column.to_string(),
                    table_b_ref_column: table_b_ref_column.to_string(),
                },
            )
            .await
        } else {
            self.difference_with_csv_file_small(
                file_b_path,
                difference_type,
                table_a_ref_column,
                table_b_ref_column,
            )
            .await
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.difference_with_csv_builder(csv_b_builder, "NORMAL", "id", "id", "75").await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn difference_with_csv_builder(
        &mut self,
        file_b_builder: CsvBuilder,
        difference_type: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
        size_threshold: &str,
    ) -> &mut Self {
        // Nested function to calculate the size of the CsvBuilder object.
        fn calculate_size(csv_builder: &CsvBuilder) -> usize {
            let headers_size = csv_builder.headers.iter().map(|h| h.len()).sum::<usize>();
            let data_size = csv_builder
                .data
                .iter()
                .map(|row| row.iter().map(|cell| cell.len()).sum::<usize>())
                .sum::<usize>();
            let limit_size = std::mem::size_of_val(&csv_builder.limit);
            let error_size = match &csv_builder.error {
                Some(err) => std::mem::size_of_val(err),
                None => 0,
            };
            headers_size + data_size + limit_size + error_size
        }

        // Convert the size threshold string to bytes (MB to bytes).
        let size_threshold_mb: usize = size_threshold.parse().unwrap_or(75); // Default to 75 MB if parsing fails
        let size_threshold_bytes = size_threshold_mb * 1024 * 1024;

        let size = calculate_size(self);

        if size > size_threshold_bytes {
            self.difference_with_csv_builder_big(
                file_b_builder,
                DaskDifferentiatorConfig {
                    difference_type: difference_type.to_string(),
                    table_a_ref_column: table_a_ref_column.to_string(),
                    table_b_ref_column: table_b_ref_column.to_string(),
                },
            )
            .await
        } else {
            self.difference_with_csv_builder_small(
                file_b_builder,
                difference_type,
                table_a_ref_column,
                table_b_ref_column,
            )
            .await
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.difference_with_csv_file_small(&csv_path_b_str, "NORMAL", "id", "id").await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn difference_with_csv_file_small(
        &mut self,
        file_b_path: &str,
        difference_type: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
    ) -> &mut Self {
        // Store the reference columns in a Vec<&str>
        let key_columns = vec![table_a_ref_column, table_b_ref_column];

        let temp_builder = Self::from_csv(file_b_path);

        // Check for loading errors
        if temp_builder.error.is_some() {
            self.error = temp_builder.error;
            return self;
        }

        let key_columns_set: HashSet<&str> = key_columns.iter().copied().collect();

        if !key_columns_set.iter().all(|&k| {
            self.headers.contains(&k.to_string()) && temp_builder.headers.contains(&k.to_string())
        }) {
            self.error = Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Key columns must exist in both datasets",
            )));
            return self;
        }

        let self_key_indices: Vec<usize> = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if key_columns_set.contains(header.as_str()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let temp_key_indices: Vec<usize> = temp_builder
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if key_columns_set.contains(header.as_str()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        // Instead of creating a HashSet of keys directly from self.data, collect keys to keep or remove first.
        let other_keys: HashSet<Vec<&String>> = temp_builder
            .data
            .iter()
            .map(|row| {
                temp_key_indices
                    .iter()
                    .map(|&i| &row[i])
                    .collect::<Vec<_>>()
            })
            .collect();

        match difference_type {
            "NORMAL" => {
                let rows_to_keep: Vec<usize> = self
                    .data
                    .iter()
                    .enumerate()
                    .filter_map(|(index, row)| {
                        let row_keys = self_key_indices
                            .iter()
                            .map(|&i| &row[i])
                            .collect::<Vec<_>>();
                        if !other_keys.contains(&row_keys) {
                            Some(index)
                        } else {
                            None
                        }
                    })
                    .collect();

                // Retain rows based on collected indices.
                // This part avoids the direct mutable borrow of self.data while it's also immutably borrowed.
                let mut new_data = Vec::new();
                for index in rows_to_keep {
                    if let Some(row) = self.data.get(index) {
                        new_data.push(row.clone());
                    }
                }

                self.data = new_data;
            }

            "SYMMETRIC" => {
                let self_keys: HashSet<Vec<&String>> = self
                    .data
                    .iter()
                    .map(|row| {
                        self_key_indices
                            .iter()
                            .map(|&i| &row[i])
                            .collect::<Vec<_>>()
                    })
                    .collect();

                // Determine the symmetric difference of keys
                let symmetric_difference_keys: HashSet<_> = self_keys
                    .symmetric_difference(&other_keys)
                    .cloned()
                    .collect();

                // Collect rows from both datasets based on symmetric difference keys
                let new_data: Vec<Vec<String>> = self
                    .data
                    .iter()
                    .chain(temp_builder.data.iter())
                    .filter(|row| {
                        let row_keys = if self.data.contains(row) {
                            self_key_indices
                                .iter()
                                .map(|&i| &row[i])
                                .collect::<Vec<_>>()
                        } else {
                            temp_key_indices
                                .iter()
                                .map(|&i| &row[i])
                                .collect::<Vec<_>>()
                        };
                        symmetric_difference_keys.contains(&row_keys)
                    })
                    .cloned()
                    .collect();

                // Update current data
                self.data = new_data;
            }

            _ => {
                println!("Unsupported difference operation type: {}", difference_type);
            }
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.difference_with_csv_builder_small(csv_b_builder, "NORMAL", "id", "id").await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn difference_with_csv_builder_small(
        &mut self,
        file_b_builder: CsvBuilder,
        difference_type: &str,
        table_a_ref_column: &str,
        table_b_ref_column: &str,
    ) -> &mut Self {
        // Store the reference columns in a Vec<&str>
        let key_columns = vec![table_a_ref_column, table_b_ref_column];

        let temp_builder = Self::from_copy(&file_b_builder);

        // Check for loading errors
        if temp_builder.error.is_some() {
            self.error = temp_builder.error;
            return self;
        }

        let key_columns_set: HashSet<&str> = key_columns.iter().copied().collect();

        if !key_columns_set.iter().all(|&k| {
            self.headers.contains(&k.to_string()) && temp_builder.headers.contains(&k.to_string())
        }) {
            self.error = Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Key columns must exist in both datasets",
            )));
            return self;
        }

        let self_key_indices: Vec<usize> = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if key_columns_set.contains(header.as_str()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let temp_key_indices: Vec<usize> = temp_builder
            .headers
            .iter()
            .enumerate()
            .filter_map(|(i, header)| {
                if key_columns_set.contains(header.as_str()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        // Instead of creating a HashSet of keys directly from self.data, collect keys to keep or remove first.
        let other_keys: HashSet<Vec<&String>> = temp_builder
            .data
            .iter()
            .map(|row| {
                temp_key_indices
                    .iter()
                    .map(|&i| &row[i])
                    .collect::<Vec<_>>()
            })
            .collect();

        match difference_type {
            "NORMAL" => {
                // Collect keys of rows to retain.
                let rows_to_keep: Vec<usize> = self
                    .data
                    .iter()
                    .enumerate()
                    .filter_map(|(index, row)| {
                        let row_keys = self_key_indices
                            .iter()
                            .map(|&i| &row[i])
                            .collect::<Vec<_>>();
                        if !other_keys.contains(&row_keys) {
                            Some(index)
                        } else {
                            None
                        }
                    })
                    .collect();

                // Retain rows based on collected indices.
                // This part avoids the direct mutable borrow of self.data while it's also immutably borrowed.
                let mut new_data = Vec::new();
                for index in rows_to_keep {
                    if let Some(row) = self.data.get(index) {
                        new_data.push(row.clone());
                    }
                }

                self.data = new_data;
            }

            "SYMMETRIC" => {
                let self_keys: HashSet<Vec<&String>> = self
                    .data
                    .iter()
                    .map(|row| {
                        self_key_indices
                            .iter()
                            .map(|&i| &row[i])
                            .collect::<Vec<_>>()
                    })
                    .collect();

                // Determine the symmetric difference of keys
                let symmetric_difference_keys: HashSet<_> = self_keys
                    .symmetric_difference(&other_keys)
                    .cloned()
                    .collect();

                // Collect rows from both datasets based on symmetric difference keys
                let new_data: Vec<Vec<String>> = self
                    .data
                    .iter()
                    .chain(temp_builder.data.iter())
                    .filter(|row| {
                        let row_keys = if self.data.contains(row) {
                            self_key_indices
                                .iter()
                                .map(|&i| &row[i])
                                .collect::<Vec<_>>()
                        } else {
                            temp_key_indices
                                .iter()
                                .map(|&i| &row[i])
                                .collect::<Vec<_>>()
                        };
                        symmetric_difference_keys.contains(&row_keys)
                    })
                    .cloned()
                    .collect();

                // Update current data
                self.data = new_data;
            }

            _ => {
                println!("Unsupported difference operation type: {}", difference_type);
            }
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskDifferentiatorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    ///
    ///
    /// let dask_differentiator_config = DaskDifferentiatorConfig {
    ///     difference_type: "NORMAL".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.difference_with_csv_file_big(&csv_path_b_str, dask_differentiator_config).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn difference_with_csv_file_big(
        &mut self,
        file_b_path: &str,
        dask_differentiator_config: DaskDifferentiatorConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.from_copy();
        let temp_builder_b = CsvBuilder::from_csv(file_b_path);

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let _ = writeln!(temp_file_a, "{}", temp_builder_a.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    if row.len() == header_len_a {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        //println!("Skipping malformed row: {:?}", row);
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !temp_builder_b.headers.is_empty() {
                    let _ = writeln!(temp_file_b, "{}", temp_builder_b.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_b = temp_builder_b.headers.len();
                for row in &temp_builder_b.data {
                    if row.len() == header_len_b {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        //println!("Skipping malformed row: {:?}", row);
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_differentiator(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_differentiator_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskDifferentiatorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = CsvBuilder::from_csv(&csv_path_a_str);
    /// let csv_b_builder = CsvBuilder::from_csv(&csv_path_b_str);
    ///
    ///
    /// let dask_differentiator_config = DaskDifferentiatorConfig {
    ///     difference_type: "NORMAL".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.difference_with_csv_builder_big(csv_b_builder, dask_differentiator_config).await;
    ///
    ///
    /// result.print_table("75").await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn difference_with_csv_builder_big(
        &mut self,
        file_b_builder: CsvBuilder,
        dask_differentiator_config: DaskDifferentiatorConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.from_copy();

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let _ = writeln!(temp_file_a, "{}", temp_builder_a.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    if row.len() == header_len_a {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !file_b_builder.headers.is_empty() {
                    let _ = writeln!(temp_file_b, "{}", file_b_builder.headers.join(","));
                }

                // Write data to the temporary file
                let header_len_b = file_b_builder.headers.len();
                for row in &file_b_builder.data {
                    if row.len() == header_len_b {
                        let escaped_row = row
                            .iter()
                            .map(|cell| escape_csv_value(cell))
                            .collect::<Vec<_>>()
                            .join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_differentiator(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_differentiator_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// Appends a new column with a static value to the CSV data.
    ///
    /// # Arguments
    ///
    /// * `static_value` - The static value to be added to each row.
    /// * `new_column_name` - The name of the new column.
    ///
    /// # Example
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["value".to_string()];
    /// let data = vec![
    ///     vec!["1.0".to_string()],
    ///     vec!["5.0".to_string()],
    ///     vec!["7.0".to_string()],
    ///     vec!["10.0".to_string()],
    ///     vec!["15.0".to_string()],
    ///     vec!["20.0".to_string()],
    ///     vec!["25.0".to_string()],
    ///     vec!["30.0".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// csv_builder.append_static_value_column("static_value", "constant");
    ///
    /// let expected_headers = vec!["value".to_string(), "constant".to_string()];
    /// assert_eq!(csv_builder.get_headers(), Some(&expected_headers[..]));
    ///
    /// let expected_data = vec![
    ///     vec!["1.0".to_string(), "static_value".to_string()],
    ///     vec!["5.0".to_string(), "static_value".to_string()],
    ///     vec!["7.0".to_string(), "static_value".to_string()],
    ///     vec!["10.0".to_string(), "static_value".to_string()],
    ///     vec!["15.0".to_string(), "static_value".to_string()],
    ///     vec!["20.0".to_string(), "static_value".to_string()],
    ///     vec!["25.0".to_string(), "static_value".to_string()],
    ///     vec!["30.0".to_string(), "static_value".to_string()],
    /// ];
    ///
    /// assert_eq!(csv_builder.get_data(), Some(&expected_data));
    /// ```
    pub fn append_static_value_column(
        &mut self,
        static_value: &str,
        new_column_name: &str,
    ) -> &mut Self {
        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Append the static value to each row
        for row in &mut self.data {
            row.push(static_value.to_string());
        }

        self
    }

    pub fn append_derived_boolean_column(
        &mut self,
        new_column_name: &str,
        expressions: Vec<(&str, Exp)>,
        result_expression: &str,
    ) -> &mut Self {
        if let Some(index) = self.headers.iter().position(|h| h == new_column_name) {
            // Remove the column from headers
            self.headers.remove(index);

            // Remove the corresponding data from each row
            for row in &mut self.data {
                if index < row.len() {
                    row.remove(index);
                }
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Clone headers to avoid borrowing issues
        let headers_clone = self.headers.clone();

        // Create a new vector to hold the updated data
        let mut updated_data = Vec::new();

        // Iterate over each row
        for row in &self.data {
            let mut expr_results = HashMap::new();
            expr_results.insert("true", true);
            expr_results.insert("false", false);

            // Evaluate each expression
            for (expr_name, exp) in &expressions {
                if let Some(column_index) = headers_clone.iter().position(|h| h == &exp.column) {
                    if let Some(cell_value) = row.get(column_index) {
                        let result = match &exp.compare_with {
                            ExpVal::STR(value_str) => {
                                value_str.apply(cell_value, &exp.operator, &exp.compare_as)
                            }
                            ExpVal::VEC(values) => {
                                values.apply(cell_value, &exp.operator, &exp.compare_as)
                            }
                        };
                        expr_results.insert(*expr_name, result);
                    } else {
                        expr_results.insert(*expr_name, false);
                    }
                } else {
                    expr_results.insert(*expr_name, false);
                }
            }

            // Evaluate the final result expression and append to the row
            let final_result = self.evaluate_result_expression(&expr_results, result_expression);
            let mut row_clone = row.clone();
            let result_str = if final_result { "1" } else { "0" };
            row_clone.push(result_str.to_string());
            updated_data.push(row_clone);
        }

        // Replace the original data with the updated data
        self.data = updated_data;

        self
    }

    pub async fn append_derived_openai_analysis_columns(
        &mut self,
        columns_to_analyze: Vec<&str>,
        analysis_query: HashMap<String, String>,
        api_key: &str,
        model: &str,
    ) -> &mut Self {
        // Prepare to update headers based on analysis keys, avoiding duplicates
        for key in analysis_query.keys() {
            if !self.headers.contains(key) {
                self.headers.push(key.clone());
            }
        }

        // Temporary vector to hold updated rows
        let mut updated_data = Vec::new();

        for row in &self.data {
            // Create a JSON string from the columns to analyze
            let mut analysis_input = serde_json::Map::new();
            for &col in &columns_to_analyze {
                if let Some(index) = self.headers.iter().position(|h| h == col) {
                    if let Some(cell_value) = row.get(index) {
                        analysis_input.insert(col.to_string(), json!(cell_value));
                    }
                }
            }

            let analysis_input_str = serde_json::to_string(&analysis_input).unwrap_or_default();

            let analysis_results = get_openai_analysis_json(
                &analysis_input_str,
                analysis_query.clone(),
                api_key,
                model,
            )
            .await
            .unwrap_or_default();

            let combined_result = json!({
                "input": analysis_input,
                "output": analysis_results
            });

            let pretty_printed_result =
                serde_json::to_string_pretty(&combined_result).unwrap_or_default();
            println!("{}", pretty_printed_result);

            //dbg!(&analysis_results);

            // Clone the row
            let mut row_clone = row.clone();

            // Ensure row_clone has the same length as headers, filling with default values if necessary
            while row_clone.len() < self.headers.len() {
                row_clone.push("".to_string()); // Use an appropriate default value
            }

            // Update row_clone with analysis results
            for (key, value) in analysis_results.iter() {
                if let Some(index) = self.headers.iter().position(|h| h == key) {
                    row_clone[index] = value.clone(); // Now safe to directly assign since row_clone matches headers length
                }
            }

            updated_data.push(row_clone);
        }

        // Replace the original data with the updated data
        self.data = updated_data;

        self
    }

    pub async fn send_data_for_openai_batch_analysis(
        &mut self,
        columns_to_analyze: Vec<&str>,
        analysis_query: HashMap<String, String>,
        api_key: &str,
        model: &str,
        batch_description: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // Prepare JSONL file content
        let mut jsonl_content = String::new();

        for (index, row) in self.data.iter().enumerate() {
            let mut analysis_input = serde_json::Map::new();
            for &col in &columns_to_analyze {
                if let Some(header_index) = self.headers.iter().position(|h| h == col) {
                    if let Some(cell_value) = row.get(header_index) {
                        analysis_input.insert(col.to_string(), json!(cell_value));
                    }
                }
            }

            let analysis_input_str = serde_json::to_string(&analysis_input).unwrap_or_default();
            let custom_id = format!("request-{}", index + 1); // Generate a unique custom_id for each request

            let request_payload = json!({
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model,
                    "messages": [
                        {
                            "role": "system",
                            "content": format!(
                                "Your only job is to extract information from the user's content and render a json in the below format: {{ {}: 'your response' }}, Your response should take into account the following meanings of the keys: {}",
                                analysis_query.keys().map(|k| format!("'{}'", k)).collect::<Vec<String>>().join(", "),
                                analysis_query.iter().enumerate().map(|(i, (k, v))| format!("{}. {}: {}", i + 1, k, v)).collect::<Vec<String>>().join(", ")
                            )
                        },
                        {
                            "role": "user",
                            "content": analysis_input_str
                        }
                    ],
            //        "max_tokens": 1000 // Adjust this value as needed
                }
            });

            jsonl_content.push_str(&serde_json::to_string(&request_payload)?);
            jsonl_content.push('\n');
        }

        // Save JSONL content to a file
        let file_path = "/tmp/openai_batch_requests.jsonl";
        let mut file = File::create(file_path)?;
        file.write_all(jsonl_content.as_bytes())?;

        // Upload the file to OpenAI
        let upload_response = upload_file_to_openai(file_path, api_key).await?;
        let input_file_id = upload_response["id"]
            .as_str()
            .ok_or("Failed to get file ID")?;
        //dbg!(&input_file_id);
        // Create the batch
        let batch_response = create_openai_batch(input_file_id, api_key, batch_description).await?;
        let batch_id = batch_response["id"]
            .as_str()
            .ok_or("Failed to get batch ID")?;

        Ok(batch_id.to_string())
    }

    /// Retrieves your OpenAI batch analysis results, and appends them as columns. If you made your batch analysis request to OpenAI via the RGWML library, this feature will seamlessly append the keys of your analysis query as column names, and their corresponding responses from OpenAI as the row values, in the same order as the file data that was originally sent to OpenAI.
    pub async fn append_openai_batch_analysis_columns(
        &mut self,
        api_key: &str,
        output_file_id: &str,
    ) -> &mut Self {
        // Retrieve the batch file and get a reference to the temp file
        let temp_file = retrieve_openai_batch(api_key, output_file_id)
            .await
            .unwrap();
        let temp_file_path = temp_file.path().to_path_buf();

        // Read the batch file
        let file = File::open(&temp_file_path).unwrap();
        let reader = BufReader::new(file);
        let mut jsonl_data: Vec<Value> = reader
            .lines()
            .map(|line| {
                let line = line.unwrap();
                //dbg!(&line); // Debugging each line
                match serde_json::from_str(&line) {
                    Ok(value) => value,
                    Err(e) => {
                        eprintln!("Failed to parse JSON: {}", e);
                        serde_json::json!({ "error": format!("Failed to parse JSON: {}", e) })
                    }
                }
            })
            .collect();
        //dbg!(&jsonl_data);

        // Sort jsonl_data by custom_id in ascending order
        jsonl_data.sort_by_key(|entry| entry["custom_id"].as_str().unwrap_or("").to_string());

        // Function to extract JSON content between the first `{` and the last `}`
        fn extract_json(content: &str) -> Option<String> {
            let start = content.find('{')?;
            let end = content.rfind('}')?;
            Some(content[start..=end].to_string())
        }

        if let Some(first_entry) = jsonl_data.first() {
            if let Some(message_content) =
                first_entry["response"]["body"]["choices"][0]["message"]["content"].as_str()
            {
                // Print the raw message content
                //dbg!(&message_content);

                // Extract the JSON content
                if let Some(json_content) = extract_json(&message_content) {
                    //dbg!(&json_content); // Debugging the extracted JSON content

                    // Function to parse JSON content
                    fn parse_json(
                        content: &str,
                    ) -> Result<HashMap<String, Value>, serde_json::Error> {
                        serde_json::from_str(content)
                    }

                    // Attempt to parse with double quotes first
                    let analysis_results: HashMap<String, Value> = match parse_json(&json_content) {
                        Ok(results) => results,
                        Err(_) => {
                            // Replace single quotes with double quotes
                            let modified_content = json_content.replace('\'', "\"");
                            match parse_json(&modified_content) {
                                Ok(results) => results,
                                Err(e) => {
                                    eprintln!("Failed to parse JSON content: {}", e);
                                    HashMap::new()
                                }
                            }
                        }
                    };

                    for key in analysis_results.keys() {
                        if !self.headers.contains(key) {
                            self.headers.push(key.clone());
                        }
                    }

                    //dbg!(&self.headers);
                } else {
                    eprintln!("No JSON content found in message.");
                }
            }
        }

        let mut updated_data = Vec::new();

        for (i, row) in self.data.iter().enumerate() {
            if let Some(line_json) = jsonl_data.get(i) {
                let response_body = &line_json["response"]["body"];
                let choices = &response_body["choices"];
                if let Some(message_content) = choices[0]["message"]["content"].as_str() {
                    // Print the raw message content
                    //dbg!(&message_content);

                    // Extract the JSON content
                    if let Some(json_content) = extract_json(&message_content) {
                        //dbg!(&json_content); // Debugging the extracted JSON content

                        fn parse_json(
                            content: &str,
                        ) -> Result<HashMap<String, Value>, serde_json::Error>
                        {
                            serde_json::from_str(content)
                        }

                        // Attempt to parse with double quotes first
                        let analysis_results: HashMap<String, String> =
                            match parse_json(&json_content) {
                                Ok(results) => results
                                    .into_iter()
                                    .map(|(k, v)| (k, v.to_string()))
                                    .collect(),
                                Err(_) => {
                                    // Replace single quotes with double quotes
                                    let modified_content = json_content.replace('\'', "\"");
                                    match parse_json(&modified_content) {
                                        Ok(results) => results
                                            .into_iter()
                                            .map(|(k, v)| (k, v.to_string()))
                                            .collect(),
                                        Err(e) => {
                                            eprintln!("Failed to parse JSON content: {}", e);
                                            HashMap::new()
                                        }
                                    }
                                }
                            };

                        // Debugging: Print analysis results
                        //dbg!(&analysis_results);

                        // Clone the row
                        let mut row_clone = row.clone();

                        // Ensure row_clone has the same length as headers, filling with default values if necessary
                        while row_clone.len() < self.headers.len() {
                            row_clone.push("".to_string()); // Use an appropriate default value
                        }

                        // Update row_clone with analysis results
                        for (key, value) in analysis_results.iter() {
                            if let Some(index) = self.headers.iter().position(|h| h == key) {
                                row_clone[index] = value.to_string(); // Now safe to directly assign since row_clone matches headers length
                            }
                        }
                        //dbg!(&row_clone);

                        updated_data.push(row_clone);
                    } else {
                        eprintln!(
                            "No JSON content found in message for custom_id: {}",
                            line_json["custom_id"]
                        );
                    }
                } else {
                    eprintln!(
                        "Message content is missing for custom_id: {}",
                        line_json["custom_id"]
                    );
                }
            } else {
                eprintln!("No JSON line found for row index: {}", i);
            }
        }

        // Replace the original data with the updated data
        self.data = updated_data;

        self
    }

    pub fn append_derived_smartcore_linear_regression_column(
        &mut self,
        new_column_name: &str,
        training_predictors: Vec<Vec<String>>,
        training_outputs: Vec<f64>,
        output_range: Vec<f64>,
        test_predictors_column_names: Vec<String>,
    ) -> &mut Self {
        //dbg!(&new_column_name, &training_predictors, &training_outputs, &output_range, &test_predictors_column_names);

        fn prepare_test_predictors(
            data: &[Vec<String>],
            headers: &[String],
            column_names: &[String],
        ) -> Vec<Vec<String>> {
            let mut predictors = Vec::new();

            // For each row, extract the values of the columns specified in `column_names`
            for row in data {
                let mut row_predictors = Vec::new();
                for column_name in column_names {
                    if let Some(index) = headers.iter().position(|h| h == column_name) {
                        if let Some(value) = row.get(index) {
                            row_predictors.push(value.clone());
                        }
                    }
                }
                predictors.push(row_predictors);
            }

            predictors
        }

        fn train_and_predict_dynamic_linear_regression(
            training_predictors: Vec<Vec<String>>,
            training_outputs: Vec<f64>,
            output_range: Vec<f64>,
            test_predictors: Vec<Vec<String>>,
        ) -> Result<Vec<f64>, Box<dyn Error>> {
            fn tap_linear_regression(
                training_inputs: &[&[f64]],
                training_outputs: Vec<f64>,
                test_inputs: &[&[f64]],
            ) -> Vec<f64> {
                // Convert training and test features into DenseMatrix

                //dbg!(&training_inputs, &training_outputs, &test_inputs);

                let training_inputs_data_matrix = DenseMatrix::from_2d_array(training_inputs);
                let test_inputs_data_matrix = DenseMatrix::from_2d_array(test_inputs);

                //dbg!(&training_inputs_data_matrix, &test_inputs_data_matrix);

                // Train the linear regression model
                let lr = LinearRegression::fit(
                    &training_inputs_data_matrix,
                    &training_outputs,
                    LinearRegressionParameters::default()
                        .with_solver(LinearRegressionSolverName::QR),
                )
                .unwrap();

                // Predict final grades for new student data
                let predicted_grades = lr.predict(&test_inputs_data_matrix).unwrap();

                predicted_grades
            }

            fn levenshtein_distance(s1: &str, s2: &str) -> usize {
                // Convert both strings to lowercase for case-insensitive comparison
                let s1 = s1.to_lowercase();
                let s2 = s2.to_lowercase();

                let s1_len = s1.chars().count();
                let s2_len = s2.chars().count();
                let mut cost = vec![vec![0; s2_len + 1]; s1_len + 1];

                for i in 0..=s1_len {
                    cost[i][0] = i;
                }
                for j in 0..=s2_len {
                    cost[0][j] = j;
                }

                for i in 1..=s1_len {
                    for j in 1..=s2_len {
                        let sub_cost = if s1.chars().nth(i - 1) == s2.chars().nth(j - 1) {
                            0
                        } else {
                            1
                        };
                        cost[i][j] = *[
                            cost[i - 1][j] + 1,
                            cost[i][j - 1] + 1,
                            cost[i - 1][j - 1] + sub_cost,
                        ]
                        .iter()
                        .min()
                        .unwrap();
                    }
                }

                cost[s1_len][s2_len]
            }

            fn find_optimal_character(training_predictors: &Vec<Vec<String>>) -> char {
                let alphabet = "abcdefghijklmnopqrstuvwxyz";
                let mut max_distance = 0;
                let mut optimal_char = 'a';

                for c in alphabet.chars() {
                    let mut min_distance = usize::MAX;
                    for conversation in training_predictors.iter() {
                        for message in conversation.iter() {
                            let distance = levenshtein_distance(message, &c.to_string());
                            if distance < min_distance {
                                min_distance = distance;
                            }
                        }
                    }
                    if min_distance > max_distance {
                        max_distance = min_distance;
                        optimal_char = c;
                    }
                }

                optimal_char
            }

            fn try_parse_to_f64(s: &str) -> Option<f64> {
                s.parse::<f64>().ok()
            }

            // Function to convert a string value to f64 using either direct conversion or levenshtein distance
            fn convert_to_numerical(value: &str, optimal_char_str: &str) -> f64 {
                // Attempt to directly convert the string to f64
                if let Some(num) = try_parse_to_f64(value) {
                    num
                } else {
                    // Fallback to calculating levenshtein distance if direct conversion is not possible
                    levenshtein_distance(value, optimal_char_str) as f64
                }
            }

            // Check if there are any training predictors and calculate the required number of predictors
            if training_predictors.is_empty() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "The training predictors vector is empty.",
                )));
            }

            let required_min_predictors = training_predictors.len() / 2;
            let current_feature_length = if let Some(first_row) = training_predictors.first() {
                first_row.len()
            } else {
                0 // Default to 0 if for some reason there's no first row, should be caught by is_empty check
            };
            //dbg!(&current_feature_length);

            if current_feature_length * 2 > training_predictors.len() {
                let additional_required = required_min_predictors - current_feature_length;
                return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Insufficient training data. You need at least {} more training predictors.", additional_required),
        )));
            }

            let optimal_char = find_optimal_character(&training_predictors);
            let optimal_char_str = optimal_char.to_string();

            let numerical_training_data: Vec<Vec<f64>> = training_predictors
                .iter()
                .map(|conversation| {
                    conversation
                        .iter()
                        .map(|message| convert_to_numerical(message, &optimal_char_str))
                        .collect()
                })
                .collect();

            let numerical_test_data: Vec<Vec<f64>> = test_predictors
                .iter()
                .map(|conversation| {
                    conversation
                        .iter()
                        .map(|message| convert_to_numerical(message, &optimal_char_str))
                        .collect()
                })
                .collect();

            let borrowed_training_data: Vec<&[f64]> =
                numerical_training_data.iter().map(AsRef::as_ref).collect();
            let borrowed_test_data: Vec<&[f64]> =
                numerical_test_data.iter().map(AsRef::as_ref).collect();

            // Convert Vec<&[f64]> into &[&[f64]] for both training and test data
            let slice_of_slices_training_data: &[&[f64]] = &borrowed_training_data;
            let slice_of_slices_test_data: &[&[f64]] = &borrowed_test_data;

            let regression_results = tap_linear_regression(
                &slice_of_slices_training_data,
                training_outputs.clone(),
                &slice_of_slices_test_data,
            );

            let max_training_output = output_range
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);
            let min_training_output = output_range.iter().cloned().fold(f64::INFINITY, f64::min);

            //dbg!(&max_training_output, &min_training_output);

            // Adjust predictions to ensure they fall within the min and max range of training_outputs
            let adjusted_predictions: Vec<f64> = regression_results
                .iter()
                .map(|&x| {
                    if x < min_training_output {
                        min_training_output
                    } else if x > max_training_output {
                        max_training_output
                    } else {
                        x
                    }
                })
                .collect();

            Ok(adjusted_predictions)
        }

        if let Some(index) = self.headers.iter().position(|h| h == new_column_name) {
            // Remove the column from headers
            self.headers.remove(index);

            // Remove the corresponding data from each row
            for row in &mut self.data {
                if index < row.len() {
                    row.remove(index);
                }
            }
        }

        // First, add the new column header
        self.headers.push(new_column_name.to_string());

        // Prepare test predictors based on the column names provided
        let test_predictors =
            prepare_test_predictors(&self.data, &self.headers, &test_predictors_column_names);

        // Call the ML function to get predictions
        match train_and_predict_dynamic_linear_regression(
            training_predictors,
            training_outputs,
            output_range,
            test_predictors,
        ) {
            Ok(predictions) => {
                //dbg!(&predictions);
                // Iterate over each row and append the prediction
                for (i, row) in self.data.iter_mut().enumerate() {
                    if let Some(prediction) = predictions.get(i) {
                        row.push(prediction.to_string());
                    } else {
                        // Handle the case where there is no prediction (should not happen in theory)
                        row.push("Error".to_string());
                    }
                }
            }
            Err(e) => {
                //dbg!(&e);
                // Handle the error, e.g., by setting the error field of CsvBuilder
                self.error = Some(e);
            }
        }

        self
    }

    pub fn append_derived_category_column(
        &mut self,
        new_column_name: &str,
        categories: Vec<(&str, Vec<(&str, Exp)>, &str)>,
    ) -> &mut Self {
        if let Some(index) = self.headers.iter().position(|h| h == new_column_name) {
            // Remove the column from headers
            self.headers.remove(index);

            // Remove the corresponding data from each row
            for row in &mut self.data {
                if index < row.len() {
                    row.remove(index);
                }
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Clone headers to avoid borrowing issues
        let headers_clone = self.headers.clone();

        // Create a new vector to hold the updated data
        let mut updated_data = Vec::new();

        // Iterate over each row
        for row in &self.data {
            let mut category_assigned = false;

            for (category_name, expressions, result_expression) in &categories {
                let mut expr_results = HashMap::new();

                // Evaluate each expression in the category
                for (expr_name, exp) in expressions {
                    if let Some(column_index) = headers_clone.iter().position(|h| h == &exp.column)
                    {
                        if let Some(cell_value) = row.get(column_index) {
                            let result = match &exp.compare_with {
                                ExpVal::STR(value_str) => {
                                    value_str.apply(cell_value, &exp.operator, &exp.compare_as)
                                }
                                ExpVal::VEC(values) => {
                                    values.apply(cell_value, &exp.operator, &exp.compare_as)
                                }
                            };
                            expr_results.insert(*expr_name, result);
                        }
                    }
                }

                // Evaluate the final result expression for the category
                let final_result =
                    self.evaluate_result_expression(&expr_results, result_expression);
                if final_result {
                    let mut row_clone = row.clone();
                    row_clone.push(category_name.to_string());
                    updated_data.push(row_clone);
                    category_assigned = true;
                    break; // Exit the loop once a category is assigned
                }
            }

            // If no category is assigned, assign a default value or handle it as required
            if !category_assigned {
                let mut row_clone = row.clone();
                row_clone.push("Uncategorized".to_string()); // or handle as needed
                updated_data.push(row_clone);
            }
        }

        // Replace the original data with the updated data
        self.data = updated_data;

        self
    }

    /// Appends a column concatenting the values of the indicated column
    pub fn append_derived_concatenation_column(
        &mut self,
        new_column_name: &str,
        items_to_concatenate: Vec<&str>,
    ) -> &mut Self {
        if let Some(index) = self.headers.iter().position(|h| h == new_column_name) {
            // Remove the column from headers
            self.headers.remove(index);

            // Remove the corresponding data from each row
            for row in &mut self.data {
                if index < row.len() {
                    row.remove(index);
                }
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Create a new vector to hold the updated data
        let mut updated_data = Vec::new();

        // Iterate over each row in the data
        for row in &self.data {
            // Initialize an empty string to store concatenated result
            let mut concatenated_result = String::new();

            // Iterate over each item to be concatenated
            for item in &items_to_concatenate {
                if let Some(column_index) = self.headers.iter().position(|h| h == item) {
                    // If the item is a column name, append its value from the row
                    if let Some(cell_value) = row.get(column_index) {
                        concatenated_result.push_str(cell_value);
                    }
                } else {
                    // If the item is not a column name, append it as-is
                    concatenated_result.push_str(item);
                }
            }

            // Clone the row and append the concatenated result
            let mut row_clone = row.clone();
            row_clone.push(concatenated_result);
            updated_data.push(row_clone);
        }

        // Replace the original data with the updated data
        self.data = updated_data;

        self
    }

    /// Appends an inclusive-exclusive interval category column.
    ///
    /// Test 1: Integer Intervals
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["value".to_string()];
    /// let data = vec![
    ///     vec!["1.0".to_string()],
    ///     vec!["5.0".to_string()],
    ///     vec!["7.0".to_string()],
    ///     vec!["10.0".to_string()],
    ///     vec!["15.0".to_string()],
    ///     vec!["20.0".to_string()],
    ///     vec!["25.0".to_string()],
    ///     vec!["30.0".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// csv_builder.append_inclusive_exclusive_numerical_interval_category_column("value", "0,10,20,30", "category");
    ///
    /// let expected_headers = vec!["value".to_string(), "category".to_string()];
    /// assert_eq!(csv_builder.get_headers(), Some(&expected_headers[..]));
    ///
    /// let expected_data = vec![
    ///     vec!["1.0".to_string(), "00 to 10".to_string()],
    ///     vec!["5.0".to_string(), "00 to 10".to_string()],
    ///     vec!["7.0".to_string(), "00 to 10".to_string()],
    ///     vec!["10.0".to_string(), "10 to 20".to_string()],
    ///     vec!["15.0".to_string(), "10 to 20".to_string()],
    ///     vec!["20.0".to_string(), "20 to 30".to_string()],
    ///     vec!["25.0".to_string(), "20 to 30".to_string()],
    ///     vec!["30.0".to_string(), "20 to 30".to_string()],
    /// ];
    ///
    /// assert_eq!(csv_builder.get_data(), Some(&expected_data));
    /// ```
    ///
    /// Test 2: Decimal Intervals
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["value".to_string()];
    /// let data = vec![
    ///     vec!["1.0".to_string()],
    ///     vec!["5.0".to_string()],
    ///     vec!["7.0".to_string()],
    ///     vec!["10.0".to_string()],
    ///     vec!["15.0".to_string()],
    ///     vec!["20.0".to_string()],
    ///     vec!["25.0".to_string()],
    ///     vec!["30.0".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// csv_builder.append_inclusive_exclusive_numerical_interval_category_column("value", "0,10.0, 20.00,30", "category");
    ///
    /// let expected_headers = vec!["value".to_string(), "category".to_string()];
    /// assert_eq!(csv_builder.get_headers(), Some(&expected_headers[..]));
    ///
    /// let expected_data = vec![
    ///     vec!["1.0".to_string(), "00.00 to 10.00".to_string()],
    ///     vec!["5.0".to_string(), "00.00 to 10.00".to_string()],
    ///     vec!["7.0".to_string(), "00.00 to 10.00".to_string()],
    ///     vec!["10.0".to_string(), "10.00 to 20.00".to_string()],
    ///     vec!["15.0".to_string(), "10.00 to 20.00".to_string()],
    ///     vec!["20.0".to_string(), "20.00 to 30.00".to_string()],
    ///     vec!["25.0".to_string(), "20.00 to 30.00".to_string()],
    ///     vec!["30.0".to_string(), "20.00 to 30.00".to_string()],
    /// ];
    ///
    /// assert_eq!(csv_builder.get_data(), Some(&expected_data));
    /// ```

    pub fn append_inclusive_exclusive_numerical_interval_category_column(
        &mut self,
        column_name: &str,
        interval_points: &str,
        new_column_name: &str,
    ) -> &mut Self {
        // Parse the interval points and determine the precision
        let points: Vec<f64> = interval_points
            .split(',')
            .map(|s| s.trim().parse::<f64>().expect("Invalid interval point"))
            .collect();

        let max_non_decimal_digits = interval_points
            .split(',')
            .map(|s| s.trim().split('.').next().unwrap().len())
            .max()
            .unwrap_or(0);

        let max_decimal_points = interval_points
            .split(',')
            .map(|s| s.trim().split('.').nth(1).map_or(0, |dec| dec.len()))
            .max()
            .unwrap_or(0);

        if points.len() < 2 {
            self.error = Some(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "At least two interval points are required",
            )));
            return self;
        }

        // Find the index of the column to apply the intervals
        let column_index = match self.headers.iter().position(|h| h == column_name) {
            Some(index) => index,
            None => {
                self.error = Some(Box::new(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Column name not found",
                )));
                return self;
            }
        };

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Create a new vector to hold the updated data
        let mut updated_data = Vec::new();

        // Iterate over each row
        for row in &self.data {
            let value = row[column_index].parse::<f64>().unwrap_or(f64::NAN);
            let mut category = String::from("Uncategorized");

            for i in 0..points.len() - 1 {
                if value >= points[i] && value < points[i + 1] {
                    if max_decimal_points > 0 {
                        category = format!(
                            "{:0width$.prec$} to {:0width$.prec$}",
                            points[i],
                            points[i + 1],
                            width = max_non_decimal_digits + max_decimal_points + 1,
                            prec = max_decimal_points
                        );
                    } else {
                        category = format!(
                            "{:0width$} to {:0width$}",
                            points[i] as i64,
                            points[i + 1] as i64,
                            width = max_non_decimal_digits
                        );
                    }
                    break;
                }
            }

            if value == *points.last().unwrap() {
                if max_decimal_points > 0 {
                    category = format!(
                        "{:0width$.prec$} to {:0width$.prec$}",
                        points[points.len() - 2],
                        points.last().unwrap(),
                        width = max_non_decimal_digits + max_decimal_points + 1,
                        prec = max_decimal_points
                    );
                } else {
                    category = format!(
                        "{:0width$} to {:0width$}",
                        points[points.len() - 2] as i64,
                        *points.last().unwrap() as i64,
                        width = max_non_decimal_digits
                    );
                }
            }

            let mut row_clone = row.clone();
            row_clone.push(category);
            updated_data.push(row_clone);
        }

        // Replace the original data with the updated data
        self.data = updated_data;

        self
    }

    /// Appends an inclusive-exclusive date interval category column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["timestamp".to_string()];
    /// let data = vec![
    ///     vec!["2023-01-01".to_string()],
    ///     vec!["2023-05-15".to_string()],
    ///     vec!["2023-07-20".to_string()],
    ///     vec!["2023-10-10".to_string()],
    ///     vec!["2024-01-01".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// csv_builder.append_inclusive_exclusive_date_interval_category_column("timestamp", "2023-01-01, 2023-06-01, 2024-01-01", "category");
    ///
    /// let expected_headers = vec!["timestamp".to_string(), "category".to_string()];
    /// assert_eq!(csv_builder.get_headers(), Some(&expected_headers[..]));
    ///
    /// let expected_data = vec![
    ///     vec!["2023-01-01".to_string(), "2023-01-01 to 2023-06-01".to_string()],
    ///     vec!["2023-05-15".to_string(), "2023-01-01 to 2023-06-01".to_string()],
    ///     vec!["2023-07-20".to_string(), "2023-06-01 to 2024-01-01".to_string()],
    ///     vec!["2023-10-10".to_string(), "2023-06-01 to 2024-01-01".to_string()],
    ///     vec!["2024-01-01".to_string(), "2023-06-01 to 2024-01-01".to_string()],
    /// ];
    ///
    /// assert_eq!(csv_builder.get_data(), Some(&expected_data));
    /// ```
    pub fn append_inclusive_exclusive_date_interval_category_column(
        &mut self,
        column_name: &str,
        interval_points: &str,
        new_column_name: &str,
    ) -> &mut Self {
        fn parse_timestamp(time_str: &str) -> Result<NaiveDateTime, String> {
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S %p",
            ];

            let parsed_date = formats
                .iter()
                .find_map(|&format| NaiveDateTime::parse_from_str(time_str, format).ok())
                .or_else(|| {
                    DateTime::parse_from_rfc2822(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                })
                .or_else(|| {
                    DateTime::parse_from_rfc3339(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                })
                .or_else(|| {
                    formats
                        .iter()
                        .find_map(|&format| NaiveDate::parse_from_str(time_str, format).ok())
                        .map(|date| date.and_hms_opt(0, 0, 0))
                        .expect("REASON")
                });

            match parsed_date {
                Some(date) => Ok(date),
                None => Err(format!("Unable to parse '{}' as a timestamp", time_str)),
            }
        }

        let points: Vec<NaiveDateTime> = interval_points
            .split(',')
            .map(|s| parse_timestamp(s.trim()).expect("Invalid date interval point"))
            .collect();

        if points.len() < 2 {
            self.error = Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "At least two interval points are required",
            )));
            return self;
        }

        // Find the index of the column to apply the intervals
        let column_index = match self.headers.iter().position(|h| h == column_name) {
            Some(index) => index,
            None => {
                self.error = Some(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Column name not found",
                )));
                return self;
            }
        };

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Create a new vector to hold the updated data
        let mut updated_data = Vec::new();

        // Iterate over each row
        for row in &self.data {
            let value = match parse_timestamp(&row[column_index]) {
                Ok(v) => v,
                Err(_) => {
                    updated_data.push(row.clone());
                    continue;
                }
            };
            let mut category = String::from("Uncategorized");

            for i in 0..points.len() - 1 {
                if value >= points[i] && value < points[i + 1] {
                    category = format!("{} to {}", points[i].date(), points[i + 1].date());
                    break;
                }
            }

            if value == *points.last().unwrap() {
                category = format!(
                    "{} to {}",
                    points[points.len() - 2].date(),
                    points.last().unwrap().date()
                );
            }

            let mut row_clone = row.clone();
            row_clone.push(category);
            updated_data.push(row_clone);
        }

        // Replace the original data with the updated data
        self.data = updated_data;

        self
    }

    /// Splits a date column into category columns which can be used for pivoting
    pub fn split_date_as_appended_category_columns(
        &mut self,
        column_name: &str,
        date_format: &str,
    ) -> &mut Self {
        // Ensure the column exists
        let column_index = match self.headers.iter().position(|h| h == column_name) {
            Some(index) => index,
            None => panic!("Column not found"),
        };

        // Define potential new column names
        let year_col = format!("{}_YEAR", column_name);
        let year_month_col = format!("{}_YEAR_MONTH", column_name);
        let year_month_day_col = format!("{}_YEAR_MONTH_DAY", column_name);
        let year_month_day_hour_col = format!("{}_YEAR_MONTH_DAY_HOUR", column_name);

        // Flags to determine which columns to add
        let mut add_year = false;
        let mut add_year_month = false;
        let mut add_year_month_day = false;
        let mut add_year_month_day_hour = false;

        // Prepare updated data
        let mut updated_data = Vec::new();

        // Iterate over each row
        for row in &self.data {
            let mut row_clone = row.clone();

            if let Some(date_str) = row.get(column_index) {
                let parsed_datetime = NaiveDateTime::parse_from_str(date_str, date_format);
                let (year, month, day, hour) = match parsed_datetime {
                    Ok(datetime) => {
                        // Set flags
                        add_year = true;
                        add_year_month = true;
                        add_year_month_day = true;
                        add_year_month_day_hour = true;

                        (
                            datetime.year(),
                            datetime.month(),
                            datetime.day(),
                            Some(datetime.hour()),
                        )
                    }
                    Err(_) => {
                        match NaiveDate::parse_from_str(date_str, date_format) {
                            Ok(date) => {
                                // Set flags
                                add_year = true;
                                add_year_month = true;
                                add_year_month_day = true;

                                (date.year(), date.month(), date.day(), None)
                            }
                            Err(_) => {
                                println!("Failed to parse date: '{}'", date_str);
                                continue;
                            }
                        }
                    }
                };

                // Add new values to the row based on flags
                if add_year {
                    row_clone.push(format!("Y{}", year));
                }
                if add_year_month {
                    row_clone.push(format!("Y{}-M{:02}", year, month));
                }
                if add_year_month_day {
                    row_clone.push(format!("Y{}-M{:02}-D{:02}", year, month, day));
                }
                if add_year_month_day_hour {
                    if let Some(hour_val) = hour {
                        row_clone.push(format!(
                            "Y{}-M{:02}-D{:02}-H{:02}",
                            year, month, day, hour_val
                        ));
                    }
                }
            }

            // Add the updated row to the new data
            updated_data.push(row_clone);
        }

        // Add headers based on flags
        if add_year {
            self.headers.push(year_col);
        }
        if add_year_month {
            self.headers.push(year_month_col);
        }
        if add_year_month_day {
            self.headers.push(year_month_day_col);
        }
        if add_year_month_day_hour {
            self.headers.push(year_month_day_hour_col);
        }

        // Update the data
        self.data = updated_data;

        self
    }

    /// Appends column with fuzzai analysis
    pub fn append_fuzzai_analysis_columns(
        &mut self,
        column_to_analyze: &str,
        column_prefix: &str,
        training_data: Vec<Train>,
        word_split_param: &str,
        word_length_sensitivity_param: &str,
        get_best_param: &str,
    ) -> &mut Self {
        // Preprocess the column_to_analyze to create the base for new column names
        let column_base = column_prefix
            .to_lowercase()
            .chars()
            //.filter(|c| c.is_alphanumeric() || *c == ' ')
            .collect::<String>()
            .replace(' ', "_");

        let column_index = self
            .headers
            .iter()
            .position(|h| h == column_to_analyze)
            .unwrap();

        let mut updated_data = Vec::new();

        // Extracting the number from the word_split_param
        let split_count = word_split_param
            .split(':')
            .nth(1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(2); // Default to 2 if parsing fails

        // Extract the sensitivity value
        let sensitivity = word_length_sensitivity_param
            .split(':')
            .nth(1)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.0); // Default to 0.8 if parsing fails

        // Extracting the number from the get_best_param
        let get_best_count = get_best_param
            .split(':')
            .nth(1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1) // Default to 1 if parsing fails
            .min(3); // Ensures the count does not exceed 3

        // Append additional column headers for each rank
        for rank in 1..=get_best_count {
            self.headers
                .push(format!("{}_rank{}_fuzzai_result", column_base, rank));
            self.headers
                .push(format!("{}_rank{}_fuzzai_result_basis", column_base, rank));
            self.headers
                .push(format!("{}_rank{}_fuzzai_score", column_base, rank));
        }

        //dbg!(&self.headers);

        //dbg!(&self.headers);
        for row in &mut self.data {
            // For each rank, append empty strings for the new columns
            for _ in 1..=get_best_count {
                row.push("".to_string()); // For fuzzai_result
                row.push("".to_string()); // For fuzzai_result_basis
                row.push("0.0".to_string()); // For fuzz_score, initialized to "0.0"
            }
        }

        for row in &self.data {
            let mut top_matches = Vec::new();

            if let Some(value_to_analyze) = row.get(column_index) {
                // Generate combinations of words based on the split count
                let combinations = Self::generate_word_combinations(value_to_analyze, split_count);

                for combo in combinations {
                    for train in &training_data {
                        let score = fuzz::ratio(&combo, &train.input) as f64;
                        // Calculate word length difference
                        let word_length_difference =
                            ((combo.len() as isize) - (train.input.len() as isize)).abs() as f64;
                        // Adjust score based on word length difference
                        let adjusted_score =
                            (score as f64) * (1.0 - (sensitivity * word_length_difference / 100.0));

                        // Push each score and corresponding result into top_matches
                        top_matches.push((adjusted_score, &train.output, &train.input));
                    }
                }
            }

            // Sort and truncate the list to get the best matches
            top_matches.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
            top_matches.truncate(get_best_count);

            // Initialize longest_value as None
            let mut longest_value: Option<(&f64, &str, &str)> = None;
            for match_ in &top_matches {
                // Destructure match_ into its components
                let (score, output, input) = match_;

                // Check if this match_ has the longest input string
                if longest_value.is_none() || input.len() > longest_value.unwrap().2.len() {
                    // Update longest_value with references to the components of match_
                    longest_value = Some((score, output, input));
                }
            }

            // Collect the indices and inputs of top_matches in a separate vector
            let match_indices_and_inputs: Vec<(usize, &str)> = top_matches
                .iter()
                .enumerate()
                .map(|(index, match_)| (index, &match_.2 as &str)) // Borrow match_.2 as a string slice
                .collect();

            // Check if the fuzz value of the longest value is more than 85 and adjust scores
            if let Some((score, _, longest_input)) = longest_value {
                if *score > 85.0 {
                    for (index, match_input) in match_indices_and_inputs {
                        if match_input == longest_input {
                            top_matches[index].0 = 100.0; // Boost the longest value's score to 100
                        }
                        /*
                        else {
                            top_matches[index].0 *= 0.95; // Decrement other values' scores
                        }
                        */
                    }
                }
            }

            let mut row_clone = row.clone();
            //dbg!(&top_matches, &row_clone);

            // Calculate the starting index for fuzzai results in row_clone
            let fuzzai_results_start_index = row_clone.len() - (get_best_count * 3);

            // Update the placeholders with the top n results
            for (i, (score, result, basis)) in top_matches.iter().enumerate() {
                let offset = i * 3; // 3 columns per rank
                row_clone[fuzzai_results_start_index + offset] = result.to_string();
                row_clone[fuzzai_results_start_index + offset + 1] = basis.to_string();
                row_clone[fuzzai_results_start_index + offset + 2] = score.to_string();
            }

            //dbg!(&top_matches, &row_clone);

            updated_data.push(row_clone);
        }

        self.data = updated_data;

        self
    }

    fn generate_word_combinations(input: &str, min_chunk_size: usize) -> Vec<String> {
        let split_words: Vec<&str> = input.split_whitespace().collect();

        // If the input can't be split into the minimum chunk size, return the input as is
        if min_chunk_size > split_words.len() {
            return vec![input.to_string()];
        }

        let mut combinations = Vec::new();

        for chunk_size in min_chunk_size..=split_words.len() {
            for start in 0..split_words.len() {
                let end = std::cmp::min(start + chunk_size, split_words.len());
                if end > start {
                    combinations.push(split_words[start..end].join(" "));
                }
            }
        }

        combinations
    }

    pub fn append_fuzzai_analysis_columns_with_values_where(
        &mut self,
        column_to_analyze: &str,
        column_prefix: &str,
        training_data: Vec<Train>,
        word_split_param: &str,
        word_length_sensitivity_param: &str,
        get_best_param: &str,
        expressions: Vec<(&str, Exp)>,
        result_expression: &str,
    ) -> &mut Self {
        let headers_clone = self.headers.clone();
        // Preprocess the column_to_analyze to create the base for new column names
        let column_base = column_prefix
            .to_lowercase()
            .chars()
            .collect::<String>()
            .replace(' ', "_");

        let column_index = self
            .headers
            .iter()
            .position(|h| h == column_to_analyze)
            .unwrap();

        //let mut updated_data: Vec<Vec<String>> = Vec::new();

        // Extracting the number from the word_split_param
        let split_count = word_split_param
            .split(':')
            .nth(1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(2); // Default to 2 if parsing fails

        // Extract the sensitivity value
        let sensitivity = word_length_sensitivity_param
            .split(':')
            .nth(1)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.0); // Default to 0.8 if parsing fails

        // Append additional column headers for each rank
        let get_best_count = get_best_param
            .split(':')
            .nth(1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1)
            .min(3);

        for rank in 1..=get_best_count {
            self.headers
                .push(format!("{}_rank{}_fuzzai_result", column_base, rank));
            self.headers
                .push(format!("{}_rank{}_fuzzai_result_basis", column_base, rank));
            self.headers
                .push(format!("{}_rank{}_fuzzai_score", column_base, rank));
        }

        for row in &mut self.data {
            // For each rank, append empty strings for the new columns
            for _ in 1..=get_best_count {
                row.push("".to_string()); // For fuzzai_result
                row.push("".to_string()); // For fuzzai_result_basis
                row.push("0.0".to_string()); // For fuzz_score, initialized to "0.0"
            }
        }

        //let mut updated_data = Vec::new();
        let mut updates = Vec::new();

        for (row_index, row) in self.data.iter().enumerate() {
            //for row in &self.data {
            let mut expr_results = HashMap::new();

            expr_results.insert("true", true);
            expr_results.insert("false", false);

            for (expr_name, exp) in &expressions {
                if let Some(column_index) = headers_clone.iter().position(|h| h == &exp.column) {
                    if let Some(cell_value) = row.get(column_index) {
                        let result = match &exp.compare_with {
                            ExpVal::STR(value_str) => {
                                value_str.apply(cell_value, &exp.operator, &exp.compare_as)
                            }
                            ExpVal::VEC(values) => {
                                values.apply(cell_value, &exp.operator, &exp.compare_as)
                            }
                        };
                        expr_results.insert(expr_name, result);
                    }
                }
            }

            //dbg!(&expr_results, &result_expression);

            let result = self.evaluate_result_expression(&expr_results, result_expression);

            if result {
                //dbg!(&result);
                let mut top_matches = Vec::new();
                // [Perform fuzzai analysis and update row as in append_fuzzai_analysis_columns...]
                if let Some(value_to_analyze) = row.get(column_index) {
                    //dbg!(&value_to_analyze);
                    // Generate combinations of words based on the split count
                    let combinations =
                        Self::generate_word_combinations(value_to_analyze, split_count);

                    for combo in combinations {
                        for train in &training_data {
                            let score = fuzz::ratio(&combo, &train.input) as f64;
                            // Calculate word length difference
                            let word_length_difference =
                                ((combo.len() as isize) - (train.input.len() as isize)).abs()
                                    as f64;
                            // Adjust score based on word length difference
                            let adjusted_score = (score as f64)
                                * (1.0 - (sensitivity * word_length_difference / 100.0));

                            // Push each score and corresponding result into top_matches
                            top_matches.push((adjusted_score, &train.output, &train.input));
                        }
                    }
                }

                /*
                top_matches.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
                top_matches.truncate(get_best_count);
                //dbg!(&row.get(column_index), &top_matches);
                */
                // Sort and truncate the list to get the best matches
                top_matches.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
                top_matches.truncate(get_best_count);

                // Initialize longest_value as None
                let mut longest_value: Option<(&f64, &str, &str)> = None;
                for match_ in &top_matches {
                    // Destructure match_ into its components
                    let (score, output, input) = match_;

                    // Check if this match_ has the longest input string
                    if longest_value.is_none() || input.len() > longest_value.unwrap().2.len() {
                        // Update longest_value with references to the components of match_
                        longest_value = Some((score, output, input));
                    }
                }
                // Collect the indices and inputs of top_matches in a separate vector
                let match_indices_and_inputs: Vec<(usize, &str)> = top_matches
                    .iter()
                    .enumerate()
                    .map(|(index, match_)| (index, &match_.2 as &str)) // Borrow match_.2 as a string slice
                    .collect();

                // Check if the fuzz value of the longest value is more than 85 and adjust scores
                if let Some((score, _, longest_input)) = longest_value {
                    if *score > 85.0 {
                        for (index, match_input) in match_indices_and_inputs {
                            if match_input == longest_input {
                                top_matches[index].0 = 100.0; // Boost the longest value's score to 100
                            } else {
                                top_matches[index].0 *= 0.95; // Decrement other values' scores
                            }
                        }
                    }
                }

                //updated_data.push(row_clone);
                updates.push((row_index, top_matches));
            }
        }

        //self.data = updated_data;

        // Step 2: Apply updates
        for (row_index, top_matches) in updates {
            let row = &mut self.data[row_index];
            let fuzzai_results_start_index = row.len() - (get_best_count * 3);

            for (i, (score, result, basis)) in top_matches.iter().enumerate() {
                let offset = i * 3; // 3 columns per rank
                row[fuzzai_results_start_index + offset] = result.to_string();
                row[fuzzai_results_start_index + offset + 1] = basis.to_string();
                row[fuzzai_results_start_index + offset + 2] = score.to_string();
            }
        }

        self
    }

    /// Evaluates truth statements
    fn evaluate_result_expression(
        &self,
        expr_results: &HashMap<&str, bool>,
        result_expression: &str,
    ) -> bool {
        let mut expression = result_expression.to_string();

        let evaluate_simple_expr = |expr: &str, expr_results: &HashMap<&str, bool>| -> bool {
            // dbg!(&expr, &expr_results);
            let result = expr
                .split_whitespace()
                .fold((None, None), |(acc, last_op), token| match token {
                    "&&" => (acc, Some("&&")),
                    "||" => (acc, Some("||")),
                    _ => {
                        let expr_result = *expr_results.get(token).unwrap_or(&false);
                        match (acc, last_op) {
                            (None, _) => (Some(expr_result), None),
                            (Some(acc_value), Some("&&")) => (Some(acc_value && expr_result), None),
                            (Some(acc_value), Some("||")) => (Some(acc_value || expr_result), None),
                            _ => (acc, None),
                        }
                    }
                })
                .0
                .unwrap_or(false);
            //dbg!(&result);
            result
        };

        // Function to extract and evaluate expressions within round brackets
        let process_brackets = |expr: &mut String, expr_results: &HashMap<&str, bool>| {
            while let Some(start) = expr.find('(') {
                if let Some(end) = expr[start..].find(')') {
                    let inner_expr = &expr[start + 1..start + end];
                    let result = evaluate_simple_expr(inner_expr, expr_results); // Evaluate the inner expression
                                                                                 //dbg!(&expr);
                    expr.replace_range(start..start + end + 1, &result.to_string());
                    //dbg!(&expr);
                    // Replace the evaluated part in the original expression
                }
            }
        };

        // Process round brackets
        process_brackets(&mut expression, expr_results);

        // Evaluate the final expression
        evaluate_simple_expr(&expression, expr_results)
    }

    /// Terminates the application. This method calls `std::process::exit(0)`, which will stop the program immediately. As such, it's typically not used in regular application logic outside of error handling or specific shutdown conditions.
    #[allow(unreachable_code)]
    pub fn die(&mut self) -> &mut Self {
        println!("Giving up the ghost!");
        std::process::exit(0);
        self
    }

    /// Checks if the CSV builder contains any data (either headers or rows).
    ///
    /// # Returns
    /// Returns `true` if there is any data in the headers or rows, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![vec!["2023-01-30".to_string(), "23.5".to_string()]]
    /// );
    ///
    /// assert!(builder.has_data());
    /// ```
    pub fn has_data(&self) -> bool {
        !self.headers.is_empty() || !self.data.is_empty()
    }

    /// Checks if the CSV builder contains headers.
    ///
    /// # Returns
    /// Returns `true` if headers are present, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![]
    /// );
    ///
    /// assert!(builder.has_headers());
    /// ```
    pub fn has_headers(&self) -> bool {
        !self.headers.is_empty()
    }

    /// Retrieves a reference to the headers of the CSV if any headers exist.
    ///
    /// # Returns
    /// Returns `Option<&[String]>` where `Some` contains a reference to the header strings if present, `None` if no headers are set.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![]
    /// );
    ///
    /// assert_eq!(builder.get_headers().unwrap(), &["date", "temperature"]);
    /// ```
    pub fn get_headers(&self) -> Option<&[String]> {
        if self.has_headers() {
            Some(&self.headers)
        } else {
            None
        }
    }

    /// Retrieves a reference to the data stored in the CSV builder if any data exists.
    ///
    /// # Returns
    /// Returns `Option<&Vec<Vec<String>>>` where `Some` contains a reference to the data rows if present, `None` if no data rows are set.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![vec!["2023-01-30".to_string(), "23.5".to_string()]]
    /// );
    ///
    /// assert_eq!(builder.get_data().unwrap(), &vec![vec!["2023-01-30".to_string(), "23.5".to_string()]]);
    ///
    /// let empty_builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![]
    /// );
    ///
    /// assert!(empty_builder.get_data().is_none());
    /// ```
    ///
    /// # Note
    /// This method is used to safely access the raw data for processing or analysis without assuming its presence.
    pub fn get_data(&self) -> Option<&Vec<Vec<String>>> {
        if !self.data.is_empty() {
            Some(&self.data)
        } else {
            None
        }
    }

    /// Returns the minimum numeric value in a column
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string(), "measured_at".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string(), "2023-01-30 05:00:00".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string(), "2023-01-30 05:31:00".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string(), "2023-02-01 09:45:00".to_string()],
    ///     ],
    /// );
    ///
    /// // Test the `get_numeric_min` method for the 'temperature' column
    /// assert_eq!(builder.get_numeric_min("temperature").unwrap(), 19.0);
    /// assert_eq!(builder.get_numeric_min("measured_at"), None); // No numeric values in 'measured_at' column
    /// ```
    pub fn get_numeric_min(&self, column_name: &str) -> Option<f64> {
        let column_index = self.headers.iter().position(|header| header == column_name);
        if let Some(col_index) = column_index {
            let mut values: Vec<f64> = self
                .data
                .iter()
                .filter_map(|row| row.get(col_index).and_then(|val| val.parse::<f64>().ok()))
                .collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            values.first().cloned()
        } else {
            None
        }
    }

    /// Returns the maximum numeric value in a column
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string(), "measured_at".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string(), "2023-01-30 05:00:00".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string(), "2023-01-30 05:31:00".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string(), "2023-02-01 09:45:00".to_string()],
    ///     ],
    /// );
    ///
    /// // Test the `get_numeric_max` method for the 'temperature' column
    /// assert_eq!(builder.get_numeric_max("temperature").unwrap(), 24.1);
    /// assert_eq!(builder.get_numeric_max("measured_at"), None); // No numeric values in 'measured_at' column
    /// ```
    pub fn get_numeric_max(&self, column_name: &str) -> Option<f64> {
        let column_index = self.headers.iter().position(|header| header == column_name);
        if let Some(col_index) = column_index {
            let mut values: Vec<f64> = self
                .data
                .iter()
                .filter_map(|row| row.get(col_index).and_then(|val| val.parse::<f64>().ok()))
                .collect();
            values.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
            values.first().cloned()
        } else {
            None
        }
    }

    /// Returns the maximum datetime value in a column
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use chrono::NaiveDateTime;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "timestamp".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "2023-01-30 05:00:00".to_string()],
    ///         vec!["2023-01-30".to_string(), "2023-01-30 05:31:00".to_string()],
    ///         vec!["2023-02-01".to_string(), "2023-02-01 09:45:00".to_string()],
    ///     ],
    /// );
    ///
    /// // Test the `get_datetime_max` method for the 'timestamp' column
    /// assert_eq!(
    ///     builder.get_datetime_max("timestamp").unwrap(),
    ///     "2023-02-01 09:45:00".to_string()
    /// );
    /// assert_eq!(builder.get_datetime_max("date"), None); // No datetime values in 'date' column
    /// ```
    pub fn get_datetime_max(&self, column_name: &str) -> Option<String> {
        let headers = match self.get_headers() {
            Some(headers) => headers,
            None => return None,
        };
        let idx = headers.iter().position(|header| header == column_name)?;

        let data = match self.get_data() {
            Some(data) => data,
            None => return None,
        };

        let mut max_datetime: Option<NaiveDateTime> = None;

        for row in data {
            if let Some(val) = row.get(idx) {
                for format in &[
                    "%Y-%m-%d",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y/%m/%d",
                    "%d-%m-%Y",
                    "%Y-%m-%d %H:%M:%S%.f",
                    "%b %d, %Y",
                ] {
                    if let Ok(datetime) = NaiveDateTime::parse_from_str(val, format) {
                        if max_datetime.map_or(true, |max| datetime > max) {
                            max_datetime = Some(datetime);
                        }
                        break; // Break once a valid format is found
                    }
                }
            }
        }

        max_datetime.map(|dt| dt.to_string())
    }

    /// Returns the minimum datetime value in a column
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use chrono::NaiveDateTime;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "timestamp".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "2023-01-30 05:00:00".to_string()],
    ///         vec!["2023-01-30".to_string(), "2023-01-30 05:31:00".to_string()],
    ///         vec!["2023-02-01".to_string(), "2023-02-01 09:45:00".to_string()],
    ///     ],
    /// );
    ///
    /// // Test the `get_datetime_min` method for the 'timestamp' column
    /// assert_eq!(
    ///     builder.get_datetime_min("timestamp").unwrap(),
    ///     "2023-01-30 05:00:00".to_string()
    /// );
    /// assert_eq!(builder.get_datetime_min("date"), None); // No datetime values in 'date' column
    /// ```
    pub fn get_datetime_min(&self, column_name: &str) -> Option<String> {
        let headers = match self.get_headers() {
            Some(headers) => headers,
            None => return None,
        };
        let idx = match headers.iter().position(|header| header == column_name) {
            Some(idx) => idx,
            None => return None,
        };

        let data = match self.get_data() {
            Some(data) => data,
            None => return None,
        };

        let mut min_datetime: Option<NaiveDateTime> = None;

        for row in data {
            if let Some(val) = row.get(idx) {
                for format in &[
                    "%Y-%m-%d",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y/%m/%d",
                    "%d-%m-%Y",
                    "%Y-%m-%d %H:%M:%S%.f",
                    "%b %d, %Y",
                ] {
                    if let Ok(datetime) = NaiveDateTime::parse_from_str(val, format) {
                        if min_datetime.map_or(true, |min| datetime < min) {
                            min_datetime = Some(datetime);
                        }
                        break; // Break once a valid format is found
                    }
                }
            }
        }

        min_datetime.map(|dt| dt.to_string())
    }

    /// Returns the range (difference between the maximum and minimum) in a numeric column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string()],
    ///     ],
    /// );
    ///
    /// let expected_range = 5.1;
    /// let actual_range = builder.get_range("temperature").unwrap();
    /// let tolerance = 0.00001; // Define a small tolerance level for floating-point comparison
    ///
    /// assert!(
    ///     (actual_range - expected_range).abs() < tolerance,
    ///     "Expected: {}, got: {}, within tolerance: {}",
    ///     expected_range,
    ///     actual_range,
    ///     tolerance
    /// );
    /// ```
    pub fn get_range(&self, column_name: &str) -> Option<f64> {
        let find_column_index = |headers: &[String], column_name: &str| -> Option<usize> {
            headers.iter().position(|h| h == column_name)
        };

        let headers = self.get_headers().unwrap();
        let idx = find_column_index(&headers, column_name)?;

        let data = self.get_data().unwrap();
        let mut min_val: Option<f64> = None;
        let mut max_val: Option<f64> = None;

        for row in data.iter() {
            if let Some(val) = row.get(idx) {
                if let Ok(num) = val.parse::<f64>() {
                    min_val = Some(min_val.map_or(num, |min| min.min(num)));
                    max_val = Some(max_val.map_or(num, |max| max.max(num)));
                }
            }
        }

        if let (Some(min), Some(max)) = (min_val, max_val) {
            Some(max - min)
        } else {
            None
        }
    }

    /// Returns the sum of all numeric values in a column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string()],
    ///     ],
    /// );
    ///
    /// // Test the `get_sum` method for the 'temperature' column
    /// assert_eq!(builder.get_sum("temperature").unwrap(), 66.6);
    /// ```
    pub fn get_sum(&self, column_name: &str) -> Option<f64> {
        let find_column_index = |headers: &[String], column_name: &str| -> Option<usize> {
            headers.iter().position(|h| h == column_name)
        };

        let headers = self.get_headers().unwrap();
        let idx = find_column_index(&headers, column_name)?;

        let data = self.get_data().unwrap();
        let mut sum = 0.0;

        for row in data.iter() {
            if let Some(val) = row.get(idx) {
                if let Ok(num) = val.parse::<f64>() {
                    sum += num;
                }
            }
        }

        Some(sum)
    }

    /// Returns the mean (average) value of all numeric values in a column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string()],
    ///     ],
    /// );
    ///
    /// // Test the `get_mean` method for the 'temperature' column
    /// assert_eq!(builder.get_mean("temperature").unwrap(), 22.2);
    /// ```
    pub fn get_mean(&self, column_name: &str) -> Option<f64> {
        let find_column_index = |headers: &[String], column_name: &str| -> Option<usize> {
            headers.iter().position(|h| h == column_name)
        };

        let headers = self.get_headers().unwrap();
        let idx = find_column_index(&headers, column_name)?;

        let data = self.get_data().unwrap();
        let mut sum = 0.0;
        let mut count = 0;

        for row in data.iter() {
            if let Some(val) = row.get(idx) {
                if let Ok(num) = val.parse::<f64>() {
                    sum += num;
                    count += 1;
                }
            }
        }

        if count > 0 {
            Some(sum / count as f64)
        } else {
            None
        }
    }

    /// Returns the median value of all numeric values in a column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string()],
    ///     ],
    /// );
    ///
    /// // Test the `get_median` method for the 'temperature' column
    /// assert_eq!(builder.get_median("temperature").unwrap(), 23.5);
    /// ```
    pub fn get_median(&self, column_name: &str) -> Option<f64> {
        let find_column_index = |headers: &[String], column_name: &str| -> Option<usize> {
            headers.iter().position(|h| h == column_name)
        };

        let headers = self.get_headers().unwrap();
        let idx = find_column_index(&headers, column_name)?;

        let data = self.get_data().unwrap();
        let mut values = Vec::new();

        for row in data.iter() {
            if let Some(val) = row.get(idx) {
                if let Ok(num) = val.parse::<f64>() {
                    values.push(num);
                }
            }
        }

        if values.is_empty() {
            return None;
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let mid = values.len() / 2;

        if values.len() % 2 == 0 {
            Some((values[mid - 1] + values[mid]) / 2.0)
        } else {
            Some(values[mid])
        }
    }

    /// Returns the mode (most frequent value) in a column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///     ],
    /// );
    ///
    /// // Test the `get_mode` method for the 'temperature' column
    /// assert_eq!(builder.get_mode("temperature").unwrap(), "23.5");
    /// ```
    pub fn get_mode(&self, column_name: &str) -> Option<String> {
        let find_column_index = |headers: &[String], column_name: &str| -> Option<usize> {
            headers.iter().position(|h| h == column_name)
        };

        let headers = self.get_headers().unwrap();
        let idx = find_column_index(&headers, column_name)?;

        let data = self.get_data().unwrap();
        let mut frequency_map = HashMap::new();

        for row in data.iter() {
            if let Some(val) = row.get(idx) {
                *frequency_map.entry(val.clone()).or_insert(0) += 1;
            }
        }

        frequency_map
            .into_iter()
            .max_by_key(|&(_, count)| count)
            .map(|(val, _)| val)
    }

    /// Returns the variance of all numeric values in a column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string()],
    ///     ],
    /// );
    ///
    /// let expected_variance = 5.18;
    /// let actual_variance = builder.get_variance("temperature").unwrap();
    /// let tolerance = 0.01; // Define a small tolerance level
    ///
    /// assert!(
    ///     (actual_variance - expected_variance).abs() < tolerance,
    ///     "Expected: {}, got: {}, within tolerance: {}",
    ///     expected_variance,
    ///     actual_variance,
    ///     tolerance
    /// );
    /// ```
    pub fn get_variance(&self, column_name: &str) -> Option<f64> {
        let find_column_index = |headers: &[String], column_name: &str| -> Option<usize> {
            headers.iter().position(|h| h == column_name)
        };

        let headers = self.get_headers().unwrap();
        let idx = find_column_index(&headers, column_name)?;

        let data = self.get_data().unwrap();
        let mut values = Vec::new();

        for row in data.iter() {
            if let Some(val) = row.get(idx) {
                if let Ok(num) = val.parse::<f64>() {
                    values.push(num);
                }
            }
        }

        if values.is_empty() {
            return None;
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance =
            values.iter().map(|&val| (val - mean).powi(2)).sum::<f64>() / values.len() as f64;
        Some(variance)
    }

    /// Returns the standard deviation of all numeric values in a column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string()],
    ///     ],
    /// );
    ///
    /// let expected_standard_deviation = 2.28;
    /// let actual_standard_deviation = builder.get_standard_deviation("temperature").unwrap();
    /// let tolerance = 0.01; // Adjusted tolerance level for floating-point comparisons
    ///
    /// assert!(
    ///     (actual_standard_deviation - expected_standard_deviation).abs() < tolerance,
    ///     "Expected: {}, got: {}, within tolerance: {}",
    ///     expected_standard_deviation,
    ///     actual_standard_deviation,
    ///     tolerance
    /// );
    /// ```
    pub fn get_standard_deviation(&self, column_name: &str) -> Option<f64> {
        let variance = self.get_variance(column_name)?;
        Some(variance.sqrt())
    }

    /// Returns the sum of squared deviations of all numeric values in a column.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string()],
    ///     ],
    /// );
    ///
    /// // Expected value calculated as approximately 15.54
    /// let expected = 15.54;
    /// let result = builder.get_sum_of_squared_deviations("temperature").unwrap();
    /// let tolerance = 1e-6; // Tolerance level for floating-point comparison
    ///
    /// // Test the `get_sum_of_squared_deviations` method for the 'temperature' column
    /// assert!(
    ///     (result - expected).abs() < tolerance,
    ///     "Expected: {}, got: {}, within tolerance: {}",
    ///     expected,
    ///     result,
    ///     tolerance
    /// );
    /// ```
    pub fn get_sum_of_squared_deviations(&self, column_name: &str) -> Option<f64> {
        let find_column_index = |headers: &[String], column_name: &str| -> Option<usize> {
            headers.iter().position(|h| h == column_name)
        };

        let headers = self.get_headers().unwrap();
        let idx = find_column_index(&headers, column_name)?;

        let data = self.get_data().unwrap();
        let mut values = Vec::new();

        for row in data.iter() {
            if let Some(val) = row.get(idx) {
                if let Ok(num) = val.parse::<f64>() {
                    values.push(num);
                }
            }
        }

        if values.is_empty() {
            return None;
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let sum_of_squared_deviations = values.iter().map(|&val| (val - mean).powi(2)).sum::<f64>();
        Some(sum_of_squared_deviations)
    }

    /// Collects and returns non-numeric values from a specified column in the dataset.
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// // Setup a CsvBuilder with some mixed data types in the 'price' column
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "price".to_string()],
    ///     vec![
    ///         vec!["2023-01-01".to_string(), "100".to_string()],
    ///         vec!["2023-01-02".to_string(), "NaN".to_string()],
    ///         vec!["2023-01-03".to_string(), "200".to_string()],
    ///         vec!["2023-01-04".to_string(), "undefined".to_string()],
    ///     ],
    /// );
    ///
    /// // Collect non-numeric values from the 'price' column
    /// let non_numeric_values = builder.get_non_numeric_values("price").unwrap();
    ///
    /// // Verify that the non-numeric values are correctly identified
    /// assert_eq!(non_numeric_values, vec!["NaN".to_string(), "undefined".to_string()]);
    /// ```
    pub fn get_non_numeric_values(&self, column_name: &str) -> Option<Vec<String>> {
        // Helper to find the index of a column
        let find_column_index = |headers: &[String], column_name: &str| -> Option<usize> {
            headers.iter().position(|h| h == column_name)
        };

        // Retrieve column index or return `None` if not found
        let headers = self.get_headers()?;
        let idx = find_column_index(&headers, column_name)?;

        // Retrieve data or return `None` if not found
        let data = self.get_data()?;
        let mut non_numeric_values = Vec::new();

        // Iterate over the data rows to identify non-numeric values
        for row in data.iter() {
            if let Some(val) = row.get(idx) {
                // Check explicitly for "NaN" and parse error cases
                if val == "NaN" || val.parse::<f64>().is_err() {
                    non_numeric_values.push(val.clone());
                }
            }
        }

        // If non-numeric values were found, return them; otherwise, return an empty vector
        Some(non_numeric_values)
    }

    /// Prints numerical analysis for the specified columns
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![
    ///         vec!["2023-01-30".to_string(), "23.5".to_string()],
    ///         vec!["2023-01-30".to_string(), "24.1".to_string()],
    ///         vec!["2023-02-01".to_string(), "19.0".to_string()],
    ///     ],
    /// );
    ///
    /// // Print numerical analysis for the 'temperature' column
    /// builder.print_column_numerical_analysis(vec!["temperature"]);
    /// ```
    pub fn print_column_numerical_analysis(&self, column_names: Vec<&str>) {
        for column_name in column_names {
            println!("Analysis for column '{}':", column_name);

            // Get non-numeric data for the column
            if let Some(non_numeric_values) = self.get_non_numeric_values(column_name) {
                if !non_numeric_values.is_empty() {
                    println!("  Non-numeric values: {:?}", non_numeric_values);
                } else {
                    println!("  Non-numeric values: None found");
                }
            } else {
                println!("  Non-numeric values: Column not found or no data available");
            }

            // Get and print the minimum value
            if let Some(min) = self.get_numeric_min(column_name) {
                println!("  Minimum: {}", min);
            } else {
                println!("  Minimum: Not found or non-numeric data");
            }

            // Get and print the maximum value
            if let Some(max) = self.get_numeric_max(column_name) {
                println!("  Maximum: {}", max);
            } else {
                println!("  Maximum: Not found or non-numeric data");
            }

            // Get and print the range
            if let Some(range) = self.get_range(column_name) {
                println!("  Range: {:.2}", range);
            } else {
                println!("  Range: Not applicable or non-numeric data");
            }

            // Get and print the sum
            if let Some(sum) = self.get_sum(column_name) {
                println!("  Sum: {:.2}", sum);
            } else {
                println!("  Sum: Not applicable or non-numeric data");
            }

            // Get and print the mean
            if let Some(mean) = self.get_mean(column_name) {
                println!("  Mean: {:.2}", mean);
            } else {
                println!("  Mean: Not applicable or non-numeric data");
            }

            // Get and print the median
            if let Some(median) = self.get_median(column_name) {
                println!("  Median: {:.2}", median);
            } else {
                println!("  Median: Not applicable or non-numeric data");
            }

            // Get and print the mode
            if let Some(mode) = self.get_mode(column_name) {
                println!("  Mode: {}", mode);
            } else {
                println!("  Mode: Not applicable or non-numeric data");
            }

            // Get and print the standard deviation
            if let Some(std_dev) = self.get_standard_deviation(column_name) {
                println!("  Standard Deviation: {:.2}", std_dev);
            } else {
                println!("  Standard Deviation: Not applicable or non-numeric data");
            }

            // Get and print the variance
            if let Some(variance) = self.get_variance(column_name) {
                println!("  Variance: {:.2}", variance);
            } else {
                println!("  Variance: Not applicable or non-numeric data");
            }

            // Get and print the sum of squared deviations
            if let Some(sum_of_sq_dev) = self.get_sum_of_squared_deviations(column_name) {
                println!("  Sum of Squared Deviations: {:.2}", sum_of_sq_dev);
            } else {
                println!("  Sum of Squared Deviations: Not applicable or non-numeric data");
            }

            //println!();
        }
    }

    pub fn print_contains_search_results(
        &mut self,
        search_string: &str,
        column_names: Vec<&str>,
    ) -> &mut Self {
        if column_names.contains(&"*") {
            // If column_names contains "*", search all columns
            let filtered_data = self
                .data
                .iter()
                .filter(|row| row.iter().any(|cell| cell.contains(search_string)))
                .cloned()
                .collect::<Vec<Vec<String>>>();
            self.data = filtered_data;
        } else {
            // Otherwise, search only specified columns
            let column_indexes: Vec<usize> = self
                .headers
                .iter()
                .enumerate()
                .filter_map(|(index, header)| {
                    if column_names.contains(&header.as_str()) {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect();

            let filtered_data = self
                .data
                .iter()
                .filter(|row| {
                    column_indexes.iter().any(|&index| {
                        row.get(index)
                            .map_or(false, |cell| cell.contains(search_string))
                    })
                })
                .cloned()
                .collect::<Vec<Vec<String>>>();
            self.data = filtered_data;
        }

        // Optionally, print the table to show the filtered data
        self.print_table_all_rows();

        self
    }

    pub fn print_not_contains_search_results(
        &mut self,
        search_string: &str,
        column_names: Vec<&str>,
    ) -> &mut Self {
        if column_names.contains(&"*") {
            // If column_names contains "*", search all columns
            let filtered_data = self
                .data
                .iter()
                .filter(|row| !row.iter().any(|cell| cell.contains(search_string)))
                .cloned()
                .collect::<Vec<Vec<String>>>();
            self.data = filtered_data;
        } else {
            // Otherwise, search only specified columns
            let column_indexes: Vec<usize> = self
                .headers
                .iter()
                .enumerate()
                .filter_map(|(index, header)| {
                    if column_names.contains(&header.as_str()) {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect();

            let filtered_data = self
                .data
                .iter()
                .filter(|row| {
                    column_indexes.iter().all(|&index| {
                        row.get(index)
                            .map_or(true, |cell| !cell.contains(search_string))
                    })
                })
                .cloned()
                .collect::<Vec<Vec<String>>>();
            self.data = filtered_data;
        }

        // Optionally, print the table to show the filtered data
        self.print_table_all_rows();

        self
    }

    pub fn print_starts_with_search_results(
        &mut self,
        search_string: &str,
        column_names: Vec<&str>,
    ) -> &mut Self {
        if column_names.contains(&"*") {
            // If column_names contains "*", search all columns
            let filtered_data = self
                .data
                .iter()
                .filter(|row| row.iter().any(|cell| cell.starts_with(search_string)))
                .cloned()
                .collect::<Vec<Vec<String>>>();
            self.data = filtered_data;
        } else {
            // Otherwise, search only specified columns
            let column_indexes: Vec<usize> = self
                .headers
                .iter()
                .enumerate()
                .filter_map(|(index, header)| {
                    if column_names.contains(&header.as_str()) {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect();

            let filtered_data = self
                .data
                .iter()
                .filter(|row| {
                    column_indexes.iter().any(|&index| {
                        row.get(index)
                            .map_or(false, |cell| cell.starts_with(search_string))
                    })
                })
                .cloned()
                .collect::<Vec<Vec<String>>>();
            self.data = filtered_data;
        }

        // Optionally, print the table to show the filtered data
        self.print_table_all_rows();

        self
    }

    pub fn print_not_starts_with_search_results(
        &mut self,
        search_string: &str,
        column_names: Vec<&str>,
    ) -> &mut Self {
        if column_names.contains(&"*") {
            // If column_names contains "*", search all columns
            let filtered_data = self
                .data
                .iter()
                .filter(|row| !row.iter().any(|cell| cell.starts_with(search_string)))
                .cloned()
                .collect::<Vec<Vec<String>>>();
            self.data = filtered_data;
        } else {
            // Otherwise, search only specified columns
            let column_indexes: Vec<usize> = self
                .headers
                .iter()
                .enumerate()
                .filter_map(|(index, header)| {
                    if column_names.contains(&header.as_str()) {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect();

            let filtered_data = self
                .data
                .iter()
                .filter(|row| {
                    column_indexes.iter().all(|&index| {
                        row.get(index)
                            .map_or(true, |cell| !cell.starts_with(search_string))
                    })
                })
                .cloned()
                .collect::<Vec<Vec<String>>>();
            self.data = filtered_data;
        }

        // Optionally, print the table to show the filtered data
        self.print_table_all_rows();

        self
    }

    pub fn print_raw_levenshtein_search_results(
        &mut self,
        search_string: &str,
        max_lev_distance: usize,
        column_names: Vec<&str>,
    ) -> &mut Self {
        fn levenshtein_distance(s1: &str, s2: &str) -> usize {
            // Convert both strings to lowercase for case-insensitive comparison
            let s1 = s1.to_lowercase();
            let s2 = s2.to_lowercase();

            let s1_len = s1.chars().count();
            let s2_len = s2.chars().count();
            let mut cost = vec![vec![0; s2_len + 1]; s1_len + 1];

            for i in 0..=s1_len {
                cost[i][0] = i;
            }
            for j in 0..=s2_len {
                cost[0][j] = j;
            }

            for i in 1..=s1_len {
                for j in 1..=s2_len {
                    let sub_cost = if s1.chars().nth(i - 1) == s2.chars().nth(j - 1) {
                        0
                    } else {
                        1
                    };
                    cost[i][j] = *[
                        cost[i - 1][j] + 1,
                        cost[i][j - 1] + 1,
                        cost[i - 1][j - 1] + sub_cost,
                    ]
                    .iter()
                    .min()
                    .unwrap();
                }
            }

            cost[s1_len][s2_len]
        }

        fn print_statistics(distances: &[usize]) {
            if distances.is_empty() {
                println!("No data to calculate statistics.");
                return;
            }

            // Sort distances to calculate median correctly and to list frequencies in order
            let mut sorted_distances = distances.to_vec();
            sorted_distances.sort_unstable();

            println!();
            let mean: f64 = sorted_distances.iter().map(|&val| val as f64).sum::<f64>()
                / sorted_distances.len() as f64;
            println!("Mean: {:.2}", mean);

            let median: f64 = if sorted_distances.len() % 2 == 0 {
                let mid = sorted_distances.len() / 2;
                (sorted_distances[mid - 1] + sorted_distances[mid]) as f64 / 2.0
            } else {
                sorted_distances[sorted_distances.len() / 2] as f64
            };
            println!("Median: {:.2}", median);

            let mut occurrences = HashMap::new();
            for &value in &sorted_distances {
                *occurrences.entry(value).or_insert(0) += 1;
            }

            let mode = occurrences
                .iter()
                .max_by_key(|&(_, count)| count)
                .map(|(&val, _)| val);
            if let Some(mode_value) = mode {
                println!("Mode: {}", mode_value);
            } else {
                println!("Mode: N/A");
            }

            // Print the frequency of each distance value from min to max
            println!("Frequencies:");
            let mut keys: Vec<_> = occurrences.keys().collect();
            keys.sort_unstable(); // Sort the keys to ensure the frequencies are printed in ascending order
            for &key in keys {
                if let Some(&count) = occurrences.get(&key) {
                    println!("  Distance {}: {} occurrences", key, count);
                }
            }
        }

        let apply_to_all_columns = column_names.len() == 1 && column_names[0] == "*";
        let column_indexes: Vec<usize> = if apply_to_all_columns {
            (0..self.headers.len()).collect()
        } else {
            self.headers
                .iter()
                .enumerate()
                .filter_map(|(index, header)| {
                    column_names.contains(&header.as_str()).then(|| index)
                })
                .collect()
        };

        let mut scored_data: Vec<(Vec<String>, usize)> = self
            .data
            .iter()
            .filter_map(|row| {
                let scores: Vec<usize> = row
                    .iter()
                    .enumerate()
                    .filter_map(|(index, cell)| {
                        if apply_to_all_columns || column_indexes.contains(&index) {
                            Some(levenshtein_distance(cell, search_string))
                        } else {
                            None
                        }
                    })
                    .collect();

                let min_distance = scores.into_iter().min().unwrap_or(usize::MAX);
                if min_distance <= max_lev_distance {
                    Some((row.clone(), min_distance))
                } else {
                    None
                }
            })
            .collect();

        // Sort by Levenshtein distance in ascending order and print table
        scored_data.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_filtered_data: Vec<Vec<String>> =
            scored_data.iter().map(|(row, _)| row.clone()).collect();
        let distances: Vec<usize> = scored_data.iter().map(|&(_, dist)| dist).collect();

        // Update the original CsvBuilder instance's data with the sorted filtered data
        self.data = sorted_filtered_data;

        // Optionally, print the table to show the sorted filtered data
        self.print_table_all_rows();

        println!();
        // Print sorted distances from min to max
        println!(
            "Distances (min to max): {}",
            distances
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );

        // Calculate and print mean, median, and mode for the distances
        print_statistics(&distances);

        self
    }

    pub fn print_vectorized_levenshtein_search_results(
        &mut self,
        search_strings: Vec<&str>,
        max_lev_distance: usize,
        column_names: Vec<&str>,
    ) -> &mut Self {
        fn levenshtein_distance(s1: &str, s2: &str) -> usize {
            // Convert both strings to lowercase for case-insensitive comparison
            let s1 = s1.to_lowercase();
            let s2 = s2.to_lowercase();

            let s1_len = s1.chars().count();
            let s2_len = s2.chars().count();
            let mut cost = vec![vec![0; s2_len + 1]; s1_len + 1];

            for i in 0..=s1_len {
                cost[i][0] = i;
            }
            for j in 0..=s2_len {
                cost[0][j] = j;
            }

            for i in 1..=s1_len {
                for j in 1..=s2_len {
                    let sub_cost = if s1.chars().nth(i - 1) == s2.chars().nth(j - 1) {
                        0
                    } else {
                        1
                    };
                    cost[i][j] = *[
                        cost[i - 1][j] + 1,
                        cost[i][j - 1] + 1,
                        cost[i - 1][j - 1] + sub_cost,
                    ]
                    .iter()
                    .min()
                    .unwrap();
                }
            }

            cost[s1_len][s2_len]
        }

        fn print_statistics(distances: &[usize]) {
            if distances.is_empty() {
                println!("No data to calculate statistics.");
                return;
            }

            // Sort distances to calculate median correctly and to list frequencies in order
            let mut sorted_distances = distances.to_vec();
            sorted_distances.sort_unstable();

            println!();
            let mean: f64 = sorted_distances.iter().map(|&val| val as f64).sum::<f64>()
                / sorted_distances.len() as f64;
            println!("Mean: {:.2}", mean);

            let median: f64 = if sorted_distances.len() % 2 == 0 {
                let mid = sorted_distances.len() / 2;
                (sorted_distances[mid - 1] + sorted_distances[mid]) as f64 / 2.0
            } else {
                sorted_distances[sorted_distances.len() / 2] as f64
            };
            println!("Median: {:.2}", median);

            let mut occurrences = HashMap::new();
            for &value in &sorted_distances {
                *occurrences.entry(value).or_insert(0) += 1;
            }

            let mode = occurrences
                .iter()
                .max_by_key(|&(_, count)| count)
                .map(|(&val, _)| val);
            if let Some(mode_value) = mode {
                println!("Mode: {}", mode_value);
            } else {
                println!("Mode: N/A");
            }

            // Print the frequency of each distance value from min to max
            println!("Frequencies:");
            let mut keys: Vec<_> = occurrences.keys().collect();
            keys.sort_unstable(); // Sort the keys to ensure the frequencies are printed in ascending order
            for &key in keys {
                if let Some(&count) = occurrences.get(&key) {
                    println!("  Distance {}: {} occurrences", key, count);
                }
            }
        }
        let apply_to_all_columns = column_names.len() == 1 && column_names[0] == "*";

        let column_indexes: Vec<usize> = if apply_to_all_columns {
            (0..self.headers.len()).collect()
        } else {
            self.headers
                .iter()
                .enumerate()
                .filter_map(|(index, header)| {
                    column_names.contains(&header.as_str()).then(|| index)
                })
                .collect()
        };

        //dbg!(&search_strings, &max_lev_distance, &column_names);

        let mut scored_data: Vec<(Vec<String>, usize)> = self
            .data
            .iter()
            .filter_map(|row| {
                let min_distance_across_search_strings: usize = search_strings
                    .iter()
                    .map(|&search_string| {
                        let search_words: Vec<&str> = search_string.split_whitespace().collect();
                        let search_word_count = search_words.len();

                        row.iter()
                            .enumerate()
                            .filter_map(|(index, cell)| {
                                if apply_to_all_columns || column_indexes.contains(&index) {
                                    let cell_words: Vec<&str> = cell.split_whitespace().collect();
                                    let cell_word_count = cell_words.len();

                                    if cell_word_count >= search_word_count {
                                        // Directly return the computed distances, avoiding nested Option
                                        return Some(
                                            (search_word_count..=cell_word_count)
                                                .map(|window_size| {
                                                    cell_words
                                                        .windows(window_size)
                                                        .map(|window| {
                                                            levenshtein_distance(
                                                                &window.join(" "),
                                                                search_string,
                                                            )
                                                        })
                                                        .min()
                                                        .unwrap_or(usize::MAX) // Handle Option<usize> from .min()
                                                })
                                                .min()
                                                .unwrap_or(usize::MAX),
                                        ); // Minimize across window sizes, then unwrap
                                    }
                                }
                                None
                            })
                            .min()
                            .unwrap_or(usize::MAX) // Minimize across all cells for this search string
                    })
                    .min()
                    .unwrap_or(usize::MAX); // Minimize across all search strings to find the overall minimum for the row

                if min_distance_across_search_strings <= max_lev_distance {
                    Some((row.clone(), min_distance_across_search_strings))
                } else {
                    None
                }
            })
            .collect();

        // Sort by Levenshtein distance in ascending order and print table
        scored_data.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_filtered_data: Vec<Vec<String>> =
            scored_data.iter().map(|(row, _)| row.clone()).collect();
        let distances: Vec<usize> = scored_data.iter().map(|&(_, dist)| dist).collect();

        // Update the original CsvBuilder instance's data with the sorted filtered data
        self.data = sorted_filtered_data;

        // Optionally, print the table to show the sorted filtered data
        self.print_table_all_rows();

        println!();
        // Print sorted distances from min to max
        println!(
            "Distances (min to max): {}",
            distances
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );

        // Calculate and print mean, median, and mode for the distances
        print_statistics(&distances);

        self
    }

    pub fn print_dot_chart(&mut self, x_axis_column: &str, y_axis_column: &str) -> &mut Self {
        let x_idx = self
            .headers
            .iter()
            .position(|h| h == x_axis_column)
            .expect("X-axis column not found");
        let y_idx = self
            .headers
            .iter()
            .position(|h| h == y_axis_column)
            .expect("Y-axis column not found");

        let mut points: Vec<(f64, f64)> = self
            .data
            .iter()
            .filter_map(|row| {
                let x = row.get(x_idx)?.parse::<f64>().ok()?;
                let y = row.get(y_idx)?.parse::<f64>().ok()?;
                Some((x, y))
            })
            .collect();

        points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // If there are more than 80 points, sample the data to get 80 representative points
        if points.len() > 80 {
            points = points
                .iter()
                .enumerate()
                .filter_map(|(i, p)| {
                    if i % (points.len() / 80) == 0 {
                        Some(*p)
                    } else {
                        None
                    }
                })
                .collect();
        }

        let width = 80; // width includes y-axis
        let height = 20; // height includes x-axis

        let x_min = points.iter().map(|p| p.0).fold(f64::INFINITY, f64::min);
        let x_max = points.iter().map(|p| p.0).fold(f64::NEG_INFINITY, f64::max);
        let y_min = points.iter().map(|p| p.1).fold(f64::INFINITY, f64::min);
        let y_max = points.iter().map(|p| p.1).fold(f64::NEG_INFINITY, f64::max);

        let x_scale = (x_max - x_min) / (width as f64 - 2.0); // Adjust for y-axis space
        let y_scale = (y_max - y_min) / (height as f64 - 2.0); // Adjust for x-axis space

        let mut chart = vec![vec![' '; width]; height];

        // Plot axes
        for y in 0..height {
            chart[y][0] = '|';
        }
        for x in 0..width {
            chart[height - 1][x] = '-';
        }
        chart[height - 1][0] = '+'; // Mark the origin

        // Plot the points
        for (x, y) in points.iter() {
            let x_pos = 1 + ((x - x_min) / x_scale) as usize; // Offset by 1 for y-axis
            let y_pos = height - 2 - ((y - y_min) / y_scale) as usize; // Adjust for x-axis
            if x_pos < width && y_pos < height {
                chart[y_pos][x_pos] = '*';
            }
        }

        // Print the chart
        for row in chart {
            let line: String = row.into_iter().collect();
            println!("  {}", line);
        }

        // Calculate and print statistical information without mode
        let y_values: Vec<f64> = points.iter().map(|&(_, y)| y).collect();

        // Mean
        let mean: f64 = y_values.iter().sum::<f64>() / y_values.len() as f64;

        // Median
        let mut sorted_y = y_values.clone();
        sorted_y.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = sorted_y.len() / 2;
        let median = if sorted_y.len() % 2 == 0 {
            (sorted_y[mid - 1] + sorted_y[mid]) / 2.0
        } else {
            sorted_y[mid]
        };

        // Print the statistical information
        println!("\n  X-Axis Range: [{}, {}]", x_min, x_max);
        println!("  Y-Axis Range: [{}, {}]", y_min, y_max);
        println!("  X-Axis Min: {}", x_min);
        println!("  X-Axis Max: {}", x_max);
        println!("  Y-Axis Min: {}", y_min);
        println!("  Y-Axis Max: {}", y_max);
        println!("  Y-Axis Mean: {:.2}", mean);
        println!("  Y-Axis Median: {:.2}", median);

        self
    }

    pub fn print_cumulative_dot_chart(
        &mut self,
        x_axis_column: &str,
        y_axis_column: &str,
    ) -> &mut Self {
        let x_idx = self
            .headers
            .iter()
            .position(|h| h == x_axis_column)
            .expect("X-axis column not found");
        let y_idx = self
            .headers
            .iter()
            .position(|h| h == y_axis_column)
            .expect("Y-axis column not found");

        let mut points: Vec<(f64, f64)> = self
            .data
            .iter()
            .filter_map(|row| {
                let x = row.get(x_idx)?.parse::<f64>().ok()?;
                let y = row.get(y_idx)?.parse::<f64>().ok()?;
                Some((x, y))
            })
            .collect();

        points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // Transform points for cumulative plotting
        let mut cumulative_sum = 0.0;
        let mut cumulative_points: Vec<(f64, f64)> = Vec::new();
        for point in points.iter() {
            cumulative_sum += point.1; // Accumulate y
            cumulative_points.push((point.0, cumulative_sum));
        }

        let width = 80; // width includes y-axis
        let height = 20; // height includes x-axis

        let x_min = cumulative_points
            .iter()
            .map(|p| p.0)
            .fold(f64::INFINITY, f64::min);
        let x_max = cumulative_points
            .iter()
            .map(|p| p.0)
            .fold(f64::NEG_INFINITY, f64::max);
        let y_min = 0.0; // Start cumulative y-axis at 0
        let y_max = cumulative_points.last().unwrap().1; // Max of cumulative sum

        let x_scale = (x_max - x_min) / (width as f64 - 2.0); // Adjust for y-axis space
        let y_scale = (y_max - y_min) / (height as f64 - 2.0); // Adjust for x-axis space

        let mut chart = vec![vec![' '; width]; height];

        // Plot axes
        for y in 0..height {
            chart[y][0] = '|';
        }
        for x in 0..width {
            chart[height - 1][x] = '-';
        }
        chart[height - 1][0] = '+'; // Mark the origin

        // Plot the cumulative points
        for (x, y) in cumulative_points.iter() {
            let x_pos = 1 + ((x - x_min) / x_scale) as usize; // Offset by 1 for y-axis
            let y_pos = height - 2 - ((y - y_min) / y_scale) as usize; // Adjust for x-axis
            if x_pos < width && y_pos < height {
                chart[y_pos][x_pos] = '*';
            }
        }

        // Print the chart
        for row in chart {
            let line: String = row.into_iter().collect();
            println!("  {}", line);
        }

        let y_min_non_zero = cumulative_points
            .iter()
            .find(|&&(_, y)| y > 0.0)
            .map(|&(_, y)| y)
            .unwrap_or(0.0);
        // Adjusted to reflect cumulative values, we'll focus on range and max
        println!("\n  X-Axis Range: [{}, {}]", x_min, x_max);
        println!(
            "  Lowest Non-Zero Cumulative Y-Axis Value: {}",
            y_min_non_zero
        );
        println!("  Cumulative Y-Axis Max: {}", y_max);

        self
    }

    pub fn print_smooth_line_chart(
        &mut self,
        x_axis_column: &str,
        y_axis_column: &str,
    ) -> &mut Self {
        let x_idx = self
            .headers
            .iter()
            .position(|h| h == x_axis_column)
            .expect("X-axis column not found");
        let y_idx = self
            .headers
            .iter()
            .position(|h| h == y_axis_column)
            .expect("Y-axis column not found");

        let points: Vec<(f64, f64)> = self
            .data
            .iter()
            .filter_map(|row| {
                let x = row.get(x_idx)?.parse::<f64>().ok()?;
                let y = row.get(y_idx)?.parse::<f64>().ok()?;
                Some((x, y))
            })
            .collect();

        let width = 80; // Includes y-axis
        let height = 20; // Includes x-axis

        let x_min = points
            .iter()
            .map(|p| p.0)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);
        let x_max = points
            .iter()
            .map(|p| p.0)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);
        let y_min = points
            .iter()
            .map(|p| p.1)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);
        let y_max = points
            .iter()
            .map(|p| p.1)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);

        let x_scale = (x_max - x_min) / (width as f64 - 2.0);
        let y_scale = (y_max - y_min) / (height as f64 - 2.0);

        let mut chart = vec![vec![' '; width]; height];

        // Draw the axes
        for y in 0..height {
            chart[y][0] = '|';
        }
        chart[height - 1] = vec!['-'; width];
        chart[height - 1][0] = '+'; // Mark the origin

        // Drawing the line
        for x_step in 1..width {
            let x_value = x_min + (x_step as f64 - 1.0) * x_scale;
            let before = points.iter().filter(|&&(x, _)| x <= x_value).last();
            let after = points.iter().filter(|&&(x, _)| x >= x_value).next();

            let y_value = match (before, after) {
                (Some(&(x0, y0)), Some(&(x1, y1))) if x0 != x1 => {
                    // Linear interpolation
                    y0 + (y1 - y0) * (x_value - x0) / (x1 - x0)
                }
                (Some(&(_, y)), _) | (_, Some(&(_, y))) => y,
                _ => continue,
            };

            let y_pos = ((y_value - y_min) / y_scale).round() as usize;
            if y_pos < height - 1 {
                chart[height - 2 - y_pos][x_step] = '*';
            }
        }

        // Ensure the x-axis is printed
        chart[height - 1][0] = '+'; // Reinforce origin to overlap with line if needed

        // Print the chart
        for row in chart {
            let line: String = row.into_iter().collect();
            println!("  {}", line);
        }

        // Extract y-values for statistical analysis
        let y_values: Vec<f64> = points.iter().map(|&(_, y)| y).collect();

        // Mean
        let mean = y_values.iter().sum::<f64>() / y_values.len() as f64;

        // Median
        let mut sorted_y = y_values.clone();
        sorted_y.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = sorted_y.len() / 2;
        let median = if sorted_y.len() % 2 == 0 {
            (sorted_y[mid - 1] + sorted_y[mid]) / 2.0
        } else {
            sorted_y[mid]
        };

        // Print the chart's statistical information
        println!("\n  X-Axis Range: [{}, {}]", x_min, x_max);
        println!("  Y-Axis Range: [{}, {}]", y_min, y_max);
        println!("  X-Axis Min: {}", x_min);
        println!("  X-Axis Max: {}", x_max);
        println!("  Y-Axis Min: {}", y_min);
        println!("  Y-Axis Max: {}", y_max);
        println!("  Y-Axis Mean: {:.2}", mean);
        println!("  Y-Axis Median: {:.2}", median);

        self
    }

    pub fn print_cumulative_smooth_line_chart(
        &mut self,
        x_axis_column: &str,
        y_axis_column: &str,
    ) -> &mut Self {
        let x_idx = self
            .headers
            .iter()
            .position(|h| h == x_axis_column)
            .expect("X-axis column not found");
        let y_idx = self
            .headers
            .iter()
            .position(|h| h == y_axis_column)
            .expect("Y-axis column not found");

        let mut points: Vec<(f64, f64)> = self
            .data
            .iter()
            .filter_map(|row| {
                let x = row.get(x_idx)?.parse::<f64>().ok()?;
                let y = row.get(y_idx)?.parse::<f64>().ok()?;
                Some((x, y))
            })
            .collect();

        points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // Cumulative transformation
        let mut cumulative_points: Vec<(f64, f64)> = Vec::new();
        let mut sum = 0.0;
        for (x, y) in points.iter() {
            sum += *y; // Accumulate y-values
            cumulative_points.push((*x, sum));
        }

        let width = 80; // Includes y-axis
        let height = 20; // Includes x-axis

        let x_min = cumulative_points
            .iter()
            .map(|p| p.0)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);
        let x_max = cumulative_points
            .iter()
            .map(|p| p.0)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);
        // Since we're working with cumulative values, start y_min at 0
        let y_min = 0.0;
        let y_max = cumulative_points.last().unwrap().1; // Max of cumulative sum

        let x_scale = (x_max - x_min) / (width as f64 - 2.0);
        let y_scale = (y_max - y_min) / (height as f64 - 2.0);

        let mut chart = vec![vec![' '; width]; height];

        // Draw the axes
        for y in 0..height {
            chart[y][0] = '|';
        }
        chart[height - 1] = vec!['-'; width];
        chart[height - 1][0] = '+'; // Mark the origin

        // Drawing the cumulative smooth line
        for x_step in 1..width {
            let x_value = x_min + (x_step as f64 - 1.0) * x_scale;
            let before = cumulative_points
                .iter()
                .filter(|&&(x, _)| x <= x_value)
                .last();
            let after = cumulative_points
                .iter()
                .filter(|&&(x, _)| x >= x_value)
                .next();

            let y_value = match (before, after) {
                (Some(&(x0, y0)), Some(&(x1, y1))) if x0 != x1 => {
                    // Linear interpolation for cumulative values
                    y0 + (y1 - y0) * (x_value - x0) / (x1 - x0)
                }
                (Some(&(_, y)), _) | (_, Some(&(_, y))) => y,
                _ => continue,
            };

            let y_pos = ((y_value - y_min) / y_scale).round() as usize;
            if y_pos < height - 1 {
                chart[height - 2 - y_pos][x_step] = '*';
            }
        }

        // Ensure the x-axis is printed
        chart[height - 1][0] = '+'; // Reinforce origin to overlap with line if needed

        // Print the chart
        for row in chart {
            let line: String = row.into_iter().collect();
            println!("  {}", line);
        }

        let y_min_non_zero = cumulative_points
            .iter()
            .find(|&&(_, y)| y > 0.0)
            .map(|&(_, y)| y)
            .unwrap_or(0.0);
        // Adjusted to reflect cumulative values, we'll focus on range and max
        println!("\n  X-Axis Range: [{}, {}]", x_min, x_max);
        println!(
            "  Lowest Non-Zero Cumulative Y-Axis Value: {}",
            y_min_non_zero
        );
        println!("  Cumulative Y-Axis Max: {}", y_max);

        self
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// let headers = vec![
    ///     "feature1".to_string(),
    ///     "feature2".to_string(),
    ///     "feature3".to_string(),
    /// ];
    ///
    /// let data = vec![
    ///     vec!["5.1".to_string(), "3.5".to_string(), "1.4".to_string()],
    ///     vec!["4.9".to_string(), "3.0".to_string(), "1.4".to_string()],
    ///     vec!["4.7".to_string(), "3.2".to_string(), "1.3".to_string()],
    ///     vec!["4.6".to_string(), "3.1".to_string(), "1.5".to_string()],
    ///     vec!["5.8".to_string(), "3.8".to_string(), "1.6".to_string()],
    ///     vec!["5.3".to_string(), "3.2".to_string(), "1.4".to_string()],
    ///     vec!["5.0".to_string(), "3.5".to_string(), "1.3".to_string()],
    ///     vec!["4.9".to_string(), "3.1".to_string(), "1.5".to_string()],
    ///     vec!["5.7".to_string(), "3.6".to_string(), "1.5".to_string()],
    ///     vec!["5.0".to_string(), "3.0".to_string(), "1.4".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers, data);
    ///
    /// let ratio = "70:20:10";
    /// builder.append_xgb_label_by_ratio_column(ratio);
    ///
    /// // Assertions
    /// let headers = builder.get_headers().unwrap();
    /// assert_eq!(headers.last().unwrap(), "XGB_TYPE");
    ///
    /// let data = builder.get_data().unwrap();
    /// let train_count = data.iter().filter(|row| row.last().unwrap() == "TRAIN").count();
    /// let validate_count = data.iter().filter(|row| row.last().unwrap() == "VALIDATE").count();
    /// let test_count = data.iter().filter(|row| row.last().unwrap() == "TEST").count();
    ///
    /// assert_eq!(train_count, 7); // 70% of 10 rows
    /// assert_eq!(validate_count, 2); // 20% of 10 rows
    /// assert_eq!(test_count, 1); // 10% of 10 rows
    ///
    /// ```
    pub fn append_xgb_label_by_ratio_column(&mut self, ratio: &str) -> &mut Self {
        let ratios: Vec<f64> = ratio
            .split(':')
            .map(|s| s.parse::<f64>().expect("Invalid ratio element"))
            .collect();

        assert_eq!(ratios.len(), 3, "Ratio must have exactly three elements");

        let total: f64 = ratios.iter().sum();
        let total_rows = self.data.len();
        let train_end = (total_rows as f64 * (ratios[0] / total)).round() as usize;
        let validate_end = train_end + (total_rows as f64 * (ratios[1] / total)).round() as usize;

        // Add new column header
        self.headers.push("XGB_TYPE".to_string());

        // Create a new vector to hold the updated data
        let mut updated_data = Vec::new();

        // Iterate over each row and assign the XGB_TYPE label
        for (i, row) in self.data.iter().enumerate() {
            let mut row_clone = row.clone();
            let xgb_type = if i < train_end {
                "TRAIN"
            } else if i < validate_end {
                "VALIDATE"
            } else {
                "TEST"
            };
            row_clone.push(xgb_type.to_string());
            updated_data.push(row_clone);
        }

        // Replace the original data with the updated data
        self.data = updated_data;

        self
    }

    ///
    /// Test 1: Using Binary Classification
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    /// use tokio::runtime::Runtime;
    /// use std::env;
    /// use std::path::PathBuf;
    ///
    ///
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    /// let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models");
    /// let model_dir_str = model_dir.to_str().unwrap();
    ///
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    /// let headers = vec![
    ///     "feature1".to_string(),
    ///     "feature2".to_string(),
    ///     "feature3".to_string(),
    ///     "target".to_string(),
    ///     "XGB_TYPE".to_string(),
    /// ];
    ///
    /// let data = vec![
    ///     vec!["5.1".to_string(), "3.5".to_string(), "1.4".to_string(), "1".to_string(), "TRAIN".to_string()],
    ///     vec!["4.9".to_string(), "3.0".to_string(), "1.4".to_string(), "0".to_string(), "TRAIN".to_string()],
    ///     vec!["4.7".to_string(), "3.2".to_string(), "1.3".to_string(), "1".to_string(), "TRAIN".to_string()],
    ///     vec!["4.6".to_string(), "3.1".to_string(), "1.5".to_string(), "0".to_string(), "TRAIN".to_string()],
    ///     vec!["5.8".to_string(), "3.8".to_string(), "1.6".to_string(), "1".to_string(), "TRAIN".to_string()],
    ///     vec!["5.3".to_string(), "3.2".to_string(), "1.4".to_string(), "0".to_string(), "TRAIN".to_string()],
    ///     vec!["5.0".to_string(), "3.5".to_string(), "1.3".to_string(), "1".to_string(), "TRAIN".to_string()],
    ///     vec!["4.9".to_string(), "3.1".to_string(), "1.5".to_string(), "0".to_string(), "TRAIN".to_string()],
    ///     vec!["5.7".to_string(), "3.6".to_string(), "1.5".to_string(), "1".to_string(), "TRAIN".to_string()],
    ///     vec!["5.0".to_string(), "3.0".to_string(), "1.4".to_string(), "0".to_string(), "TRAIN".to_string()],
    ///     vec!["5.5".to_string(), "3.4".to_string(), "1.5".to_string(), "1".to_string(), "TRAIN".to_string()],
    ///     vec!["4.8".to_string(), "3.1".to_string(), "1.4".to_string(), "0".to_string(), "TRAIN".to_string()],
    ///     vec!["5.1".to_string(), "3.8".to_string(), "1.6".to_string(), "1".to_string(), "TRAIN".to_string()],
    ///     vec!["4.9".to_string(), "3.1".to_string(), "1.5".to_string(), "0".to_string(), "TRAIN".to_string()],
    ///     vec!["5.6".to_string(), "3.4".to_string(), "1.5".to_string(), "1".to_string(), "TRAIN".to_string()],
    ///     vec!["5.0".to_string(), "3.2".to_string(), "1.4".to_string(), "0".to_string(), "TRAIN".to_string()],
    ///     vec!["5.0".to_string(), "3.6".to_string(), "1.4".to_string(), "1".to_string(), "VALIDATE".to_string()],
    ///     vec!["5.4".to_string(), "3.9".to_string(), "1.7".to_string(), "0".to_string(), "VALIDATE".to_string()],
    ///     vec!["4.6".to_string(), "3.4".to_string(), "1.4".to_string(), "1".to_string(), "VALIDATE".to_string()],
    ///     vec!["5.0".to_string(), "3.4".to_string(), "1.5".to_string(), "0".to_string(), "VALIDATE".to_string()],
    ///     vec!["5.7".to_string(), "3.8".to_string(), "1.7".to_string(), "1".to_string(), "VALIDATE".to_string()],
    ///     vec!["5.1".to_string(), "3.5".to_string(), "1.4".to_string(), "0".to_string(), "VALIDATE".to_string()],
    ///     vec!["5.5".to_string(), "3.7".to_string(), "1.5".to_string(), "1".to_string(), "VALIDATE".to_string()],
    ///     vec!["4.9".to_string(), "3.3".to_string(), "1.4".to_string(), "0".to_string(), "VALIDATE".to_string()],
    ///     vec!["5.4".to_string(), "3.9".to_string(), "1.7".to_string(), "1".to_string(), "VALIDATE".to_string()],
    ///     vec!["5.0".to_string(), "3.4".to_string(), "1.5".to_string(), "0".to_string(), "VALIDATE".to_string()],
    ///     vec!["4.4".to_string(), "2.9".to_string(), "1.4".to_string(), "1".to_string(), "TEST".to_string()],
    ///     vec!["4.9".to_string(), "3.1".to_string(), "1.5".to_string(), "0".to_string(), "TEST".to_string()],
    ///     vec!["5.6".to_string(), "3.0".to_string(), "1.3".to_string(), "1".to_string(), "TEST".to_string()],
    ///     vec!["5.1".to_string(), "3.4".to_string(), "1.5".to_string(), "0".to_string(), "TEST".to_string()],
    ///     vec!["5.7".to_string(), "3.5".to_string(), "1.6".to_string(), "1".to_string(), "TEST".to_string()],
    ///     vec!["5.2".to_string(), "3.4".to_string(), "1.4".to_string(), "0".to_string(), "TEST".to_string()],
    ///     vec!["5.8".to_string(), "3.1".to_string(), "1.7".to_string(), "1".to_string(), "TEST".to_string()],
    ///     vec!["5.0".to_string(), "3.2".to_string(), "1.5".to_string(), "0".to_string(), "TEST".to_string()],
    ///     vec!["5.9".to_string(), "3.3".to_string(), "1.7".to_string(), "1".to_string(), "TEST".to_string()],
    ///     vec!["5.3".to_string(), "3.0".to_string(), "1.4".to_string(), "0".to_string(), "TEST".to_string()],
    /// ];
    ///
    ///     let mut builder = CsvBuilder::from_raw_data(headers, data);
    ///
    ///     let param_column_names = "feature1, feature2, feature3";
    ///     let target_column_name = "target";
    ///     let prediction_column_name = "target_PREDICTION";
    ///     let model_name_str = "test_bin_class_model";
    ///     
    ///     let xgb_config = XgbConfig {
    ///         xgb_objective: "binary:logistic".to_string(),
    ///         xgb_max_depth: "5".to_string(),
    ///         xgb_learning_rate: "0.1".to_string(),
    ///         xgb_n_estimators: "100".to_string(),
    ///         xgb_gamma: "0.1".to_string(),
    ///         xgb_min_child_weight: "1".to_string(),
    ///         xgb_subsample: "0.8".to_string(),
    ///         xgb_colsample_bytree: "0.8".to_string(),
    ///         xgb_reg_lambda: "1.0".to_string(),
    ///         xgb_reg_alpha: "0.0".to_string(),
    ///         xgb_scale_pos_weight: "".to_string(),
    ///         xgb_max_delta_step: "".to_string(),
    ///         xgb_booster: "".to_string(),
    ///         xgb_tree_method: "".to_string(),
    ///         xgb_grow_policy: "".to_string(),
    ///         xgb_eval_metric: "".to_string(),
    ///         xgb_early_stopping_rounds: "".to_string(),
    ///         xgb_device: "".to_string(),
    ///         xgb_cv: "".to_string(),
    ///         xgb_interaction_constraints: "".to_string(),
    ///         hyperparameter_optimization_attempts: "".to_string(),
    ///         hyperparameter_optimization_result_display_limit: "".to_string(),
    ///         dask_workers: "".to_string(),
    ///         dask_threads_per_worker: "".to_string(),
    ///     };
    ///
    ///     builder.create_xgb_model(param_column_names, target_column_name, prediction_column_name, model_dir_str, model_name_str, xgb_config).await;
    ///     // Use get_headers and get_data methods to access private fields
    ///     let updated_headers = builder.get_headers().unwrap_or(&[]).to_vec();
    ///     let updated_data = builder.get_data().unwrap_or(&vec![]).clone();
    ///
    ///     println!("Updated Builder Headers: {:?}", updated_headers);
    ///     println!("Updated Builder Data: {:?}", updated_data);
    ///
    ///     assert_eq!(updated_headers, vec!["feature1", "feature2", "feature3", "target", "XGB_TYPE", "target_PREDICTION"]);
    ///     assert_eq!(updated_data.len(), 36);
    /// });
    /// ```

    ///
    /// Test 2: Using Linear Regression
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    /// use std::env::current_dir;
    ///
    /// // Get the current working directory
    /// let current_dir = current_dir().unwrap();
    ///
    /// // Append the relative path to the current directory
    /// let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models");
    ///
    /// // Convert the path to a string
    /// let model_dir_str = model_dir.to_str().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    /// let headers = vec![
    ///     "no_of_tickets".to_string(),
    ///     "last_60_days_tickets".to_string(),
    ///     "churn_day".to_string(),
    ///     "XGB_TYPE".to_string(),
    /// ];
    ///
    /// let data = vec![
    ///     vec!["5".to_string(), "2".to_string(), "180".to_string(), "TRAIN".to_string()],
    ///     vec!["6".to_string(), "3".to_string(), "170".to_string(), "TRAIN".to_string()],
    ///     vec!["4".to_string(), "1".to_string(), "190".to_string(), "TRAIN".to_string()],
    ///     vec!["5".to_string(), "1".to_string(), "185".to_string(), "TRAIN".to_string()],
    ///     vec!["10".to_string(), "6".to_string(), "90".to_string(), "TRAIN".to_string()],
    ///     vec!["11".to_string(), "5".to_string(), "80".to_string(), "TRAIN".to_string()],
    ///     vec!["12".to_string(), "7".to_string(), "70".to_string(), "TRAIN".to_string()],
    ///     vec!["20".to_string(), "10".to_string(), "68".to_string(), "TRAIN".to_string()],
    ///     vec!["30".to_string(), "15".to_string(), "46".to_string(), "TRAIN".to_string()],
    ///     vec!["30".to_string(), "12".to_string(), "46".to_string(), "TRAIN".to_string()],
    ///     vec!["32".to_string(), "10".to_string(), "47".to_string(), "TRAIN".to_string()],
    ///     vec!["5".to_string(), "4".to_string(), "172".to_string(), "VALIDATE".to_string()],
    ///     vec!["13".to_string(), "3".to_string(), "75".to_string(), "VALIDATE".to_string()],
    ///     vec!["35".to_string(), "8".to_string(), "47".to_string(), "VALIDATE".to_string()],
    ///     vec!["5".to_string(), "3".to_string(), "182".to_string(), "TEST".to_string()],
    ///     vec!["6".to_string(), "2".to_string(), "173".to_string(), "TEST".to_string()],
    ///     vec!["13".to_string(), "6".to_string(), "68".to_string(), "TEST".to_string()],
    ///     vec!["12".to_string(), "8".to_string(), "69".to_string(), "TEST".to_string()],
    ///     vec!["22".to_string(), "9".to_string(), "66".to_string(), "TEST".to_string()],
    ///     vec!["32".to_string(), "9".to_string(), "46".to_string(), "TEST".to_string()],
    /// ];
    ///
    ///     let mut builder = CsvBuilder::from_raw_data(headers, data);
    ///
    ///     let param_column_names = "no_of_tickets,last_60_days_tickets";
    ///     let target_column_name = "churn_day";
    ///     let prediction_column_name = "churn_day_PREDICTION";
    ///     let model_name_str = "test_reg_model";
    ///  
    ///     let xgb_config = XgbConfig {
    ///         xgb_objective: "reg:squarederror".to_string(),
    ///         xgb_max_depth: "6".to_string(),
    ///         xgb_learning_rate: "0.05".to_string(),
    ///         xgb_n_estimators: "200".to_string(),
    ///         xgb_gamma: "0.2".to_string(),
    ///         xgb_min_child_weight: "5".to_string(),
    ///         xgb_subsample: "0.8".to_string(),
    ///         xgb_colsample_bytree: "0.8".to_string(),
    ///         xgb_reg_lambda: "2.0".to_string(),
    ///         xgb_reg_alpha: "0.5".to_string(),
    ///         xgb_scale_pos_weight: "".to_string(),
    ///         xgb_max_delta_step: "".to_string(),
    ///         xgb_booster: "".to_string(),
    ///         xgb_tree_method: "".to_string(),
    ///         xgb_grow_policy: "".to_string(),
    ///         xgb_eval_metric: "".to_string(),
    ///         xgb_early_stopping_rounds: "".to_string(),
    ///         xgb_device: "".to_string(),
    ///         xgb_cv: "".to_string(),
    ///         xgb_interaction_constraints: "".to_string(),
    ///         hyperparameter_optimization_attempts: "".to_string(),
    ///         hyperparameter_optimization_result_display_limit: "".to_string(),
    ///         dask_workers: "".to_string(),
    ///         dask_threads_per_worker: "".to_string(),
    ///     };
    ///
    ///     builder.create_xgb_model(param_column_names, target_column_name, prediction_column_name, model_dir_str, model_name_str, xgb_config).await;
    ///     // Use get_headers and get_data methods to access private fields
    ///     let updated_headers = builder.get_headers().unwrap_or(&[]).to_vec();
    ///     let updated_data = builder.get_data().unwrap_or(&vec![]).clone();
    ///
    ///     println!("Updated Builder Headers: {:?}", updated_headers);
    ///     println!("Updated Builder Data: {:?}", updated_data);
    ///
    ///     assert_eq!(updated_headers, vec!["no_of_tickets", "last_60_days_tickets", "churn_day", "XGB_TYPE", "churn_day_PREDICTION"]);
    ///     assert_eq!(updated_data.len(), 20);
    /// });
    /// ```

    pub async fn create_xgb_model(
        &mut self,
        param_column_names: &str,
        target_column_name: &str,
        prediction_column_name: &str,
        model_dir_str: &str,
        model_name_str: &str,
        xgb_config: XgbConfig,
    ) -> (&mut Self, serde_json::Value) {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        // Split and trim column names into a vector
        let param_columns: Vec<&str> = param_column_names.split(',').map(|s| s.trim()).collect();
        let target_column = target_column_name.trim();

        // Create a copy of the builder
        let mut temp_builder = self.from_copy();

        // Retain only the parameter and target columns
        let columns_to_retain: Vec<&str> = {
            let mut columns = param_columns.clone();
            columns.push(target_column);
            columns.push("XGB_TYPE");
            columns
        };
        //dbg!(&columns_to_retain);
        temp_builder.retain_columns(columns_to_retain);

        // Create a temporary file
        if let Ok(mut temp_file) = NamedTempFile::new() {
            /*
            // Write headers to the temporary file
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            for row in &temp_builder.data {
                let _ = writeln!(temp_file, "{}", row.join(","));
            }
            */

            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                //println!("Temporary CSV file contents:");
                //        if let Ok(csv_contents) = std::fs::read_to_string(temp_file.path()) {
                //println!("{}", csv_contents);

                // Call the train method from XgbConnect
                if let Ok(result) = XgbConnect::train(
                    csv_path_str,
                    param_column_names,
                    target_column_name,
                    prediction_column_name,
                    model_dir_str,
                    model_name_str,
                    xgb_config,
                )
                .await
                {
                    //dbg!(&result);

                    let (headers, rows, report) = result;

                    dbg!(&prediction_column_name, &headers);

                    // Find the index of the prediction column in the result
                    let prediction_idx = headers
                        .iter()
                        .position(|h| h == prediction_column_name)
                        .unwrap();

                    // Extract the prediction values from the rows
                    let prediction_values: Vec<String> =
                        rows.iter().map(|row| row[prediction_idx].clone()).collect();

                    // Append the prediction column to the original data
                    if let Some(prediction_header_idx) = self
                        .headers
                        .iter()
                        .position(|h| h == prediction_column_name)
                    {
                        // Update existing prediction column
                        for (i, value) in prediction_values.iter().enumerate() {
                            self.data[i][prediction_header_idx] = value.clone();
                        }
                    } else {
                        // Add a new prediction column
                        self.headers.push(prediction_column_name.to_string());
                        for (i, value) in prediction_values.iter().enumerate() {
                            self.data[i].push(value.clone());
                        }
                    }

                    // Clone the headers to avoid mutable and immutable borrow conflict
                    let cloned_headers = self.headers.clone();

                    // Order columns in the original order with the new column appended
                    self.order_columns(cloned_headers.iter().map(|s| s.as_str()).collect());

                    // Process the report into a JSON object
                    let mut report_json = HashMap::new();
                    for item in report {
                        let key = item[0].clone();
                        let value = if let Ok(parsed) = serde_json::from_str::<Value>(&item[1]) {
                            parsed
                        } else {
                            Value::String(item[1].clone())
                        };
                        report_json.insert(key, value);
                    }
                    let report_json = json!(report_json);

                    // Return the updated self and the report JSON
                    return (self, report_json);
                }
                // }
            }
        }

        // Return an empty report JSON in case of failure
        let empty_report_json = json!({});

        (self, empty_report_json)
    }

    ///
    /// Test 1: Using Binary Classification
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    /// use tokio::runtime::Runtime;
    /// use std::env::current_dir;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = current_dir().unwrap();
    ///
    /// // Append the relative path to the current directory
    /// let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models/test_bin_class_model.json");
    ///
    /// // Convert the path to a string
    /// let model_path_str = model_dir.to_str().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    /// let headers = vec![
    ///     "feature1".to_string(),
    ///     "feature2".to_string(),
    ///     "feature3".to_string(),
    /// ];
    ///
    /// let data = vec![
    ///     vec!["5.1".to_string(), "3.5".to_string(), "1.4".to_string()],
    ///     vec!["4.9".to_string(), "3.0".to_string(), "1.4".to_string()],
    ///     vec!["4.7".to_string(), "3.2".to_string(), "1.3".to_string()],
    ///     vec!["4.6".to_string(), "3.1".to_string(), "1.5".to_string()],
    ///     vec!["5.8".to_string(), "3.8".to_string(), "1.6".to_string()],
    ///     vec!["5.3".to_string(), "3.2".to_string(), "1.4".to_string()],
    ///     vec!["5.0".to_string(), "3.5".to_string(), "1.3".to_string()],
    ///     vec!["4.9".to_string(), "3.1".to_string(), "1.5".to_string()],
    ///     vec!["5.7".to_string(), "3.6".to_string(), "1.5".to_string()],
    ///     vec!["5.0".to_string(), "3.0".to_string(), "1.4".to_string()],
    /// ];
    ///
    ///     let mut builder = CsvBuilder::from_raw_data(headers, data);
    ///
    ///     let param_column_names = "feature1, feature2, feature3";
    ///     let prediction_column_name = "PREDICTION";
    ///     
    ///     builder.append_xgb_model_predictions_column(param_column_names, prediction_column_name, model_path_str).await;
    ///     // Use get_headers and get_data methods to access private fields
    ///     let updated_headers = builder.get_headers().unwrap_or(&[]).to_vec();
    ///     let updated_data = builder.get_data().unwrap_or(&vec![]).clone();
    ///
    ///     println!("Updated Builder Headers: {:?}", updated_headers);
    ///     println!("Updated Builder Data: {:?}", updated_data);
    ///
    ///     assert_eq!(updated_headers, vec!["feature1", "feature2", "feature3", "PREDICTION"]);
    ///     assert_eq!(updated_data.len(), 10);
    /// });
    /// ```

    ///
    /// Test 2: Using Linear Regression
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    /// use tokio::runtime::Runtime;
    /// use std::env::current_dir;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = current_dir().unwrap();
    ///
    /// // Append the relative path to the current directory
    /// let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models/test_reg_model.json");
    ///
    /// // Convert the path to a string
    /// let model_path_str = model_dir.to_str().unwrap();
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    /// let headers = vec![
    ///     "no_of_tickets".to_string(),
    ///     "last_60_days_tickets".to_string(),
    /// ];
    ///
    /// let data = vec![
    ///     vec!["5".to_string(), "2".to_string()],
    ///     vec!["6".to_string(), "3".to_string()],
    ///     vec!["4".to_string(), "1".to_string()],
    ///     vec!["5".to_string(), "1".to_string()],
    ///     vec!["10".to_string(), "6".to_string()],
    ///     vec!["11".to_string(), "5".to_string()],
    ///     vec!["12".to_string(), "7".to_string()],
    ///     vec!["20".to_string(), "10".to_string()],
    ///     vec!["30".to_string(), "15".to_string()],
    ///     vec!["30".to_string(), "12".to_string()],
    /// ];
    ///
    ///     let mut builder = CsvBuilder::from_raw_data(headers, data);
    ///
    ///     let param_column_names = "no_of_tickets,last_60_days_tickets";
    ///     let prediction_column_name = "PREDICTION";
    ///  
    ///     builder.append_xgb_model_predictions_column(param_column_names, prediction_column_name, model_path_str).await;
    ///     // Use get_headers and get_data methods to access private fields
    ///     let updated_headers = builder.get_headers().unwrap_or(&[]).to_vec();
    ///     let updated_data = builder.get_data().unwrap_or(&vec![]).clone();
    ///
    ///     println!("Updated Builder Headers: {:?}", updated_headers);
    ///     println!("Updated Builder Data: {:?}", updated_data);
    ///
    ///     assert_eq!(updated_headers, vec!["no_of_tickets", "last_60_days_tickets", "PREDICTION"]);
    ///     assert_eq!(updated_data.len(), 10);
    /// });
    /// ```

    pub async fn append_xgb_model_predictions_column(
        &mut self,
        param_column_names: &str,
        prediction_column_name: &str,
        model_path_str: &str,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        // Split and trim column names into a vector
        let param_columns: Vec<&str> = param_column_names.split(',').map(|s| s.trim()).collect();

        // Create a copy of the builder
        let mut temp_builder = self.from_copy();

        // Retain only the parameter columns
        let columns_to_retain: Vec<&str> = {
            let columns = param_columns.clone();
            columns
        };
        temp_builder.retain_columns(columns_to_retain);

        // Create a temporary file
        if let Ok(mut temp_file) = NamedTempFile::new() {
            /*
            // Write headers to the temporary file
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            for row in &temp_builder.data {
                let _ = writeln!(temp_file, "{}", row.join(","));
            }
            */

            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                //println!("Temporary CSV file contents:");
                //if let Ok(csv_contents) = std::fs::read_to_string(temp_file.path()) {
                //println!("{}", csv_contents);

                // Call the predict method from XgbConnect
                if let Ok(result) = XgbConnect::predict(
                    csv_path_str,
                    param_column_names,
                    prediction_column_name,
                    model_path_str,
                )
                .await
                {
                    //dbg!(&result);

                    let (headers, rows) = result;

                    // Find the index of the prediction column in the result
                    let prediction_idx = headers
                        .iter()
                        .position(|h| h == prediction_column_name)
                        .unwrap();

                    // Extract the prediction values from the rows
                    let prediction_values: Vec<String> =
                        rows.iter().map(|row| row[prediction_idx].clone()).collect();

                    // Append the prediction column to the original data
                    if let Some(prediction_header_idx) = self
                        .headers
                        .iter()
                        .position(|h| h == prediction_column_name)
                    {
                        // Update existing prediction column
                        for (i, value) in prediction_values.iter().enumerate() {
                            self.data[i][prediction_header_idx] = value.clone();
                        }
                    } else {
                        // Add a new prediction column
                        self.headers.push(prediction_column_name.to_string());
                        for (i, value) in prediction_values.iter().enumerate() {
                            self.data[i].push(value.clone());
                        }
                    }

                    // Clone the headers to avoid mutable and immutable borrow conflict
                    let cloned_headers = self.headers.clone();

                    // Order columns in the original order with the new column appended
                    self.order_columns(cloned_headers.iter().map(|s| s.as_str()).collect());

                    // Return the updated self
                    return self;
                }
                // }
            }
        }

        self
    }

    /// Appends a new column with the count of timestamps after a given date.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    ///
    /// let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "timestamps".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "2023-01-01 12:30:00;2023-01-01 13:00:00".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "2022-12-31 12:00:00;2022-12-31 12:30:00".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "2023-01-02 14:00:00;2023-01-02 15:00:00".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "28".to_string(), "2023-01-01 11:00:00".to_string(), "2023-01-01 10:00:00;2023-01-01 09:00:00".to_string()]
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    /// csv_builder.append_semi_colon_separated_timestamp_count_after_date_column("timestamps", "date", "count_after_date");
    /// ```
    pub fn append_semi_colon_separated_timestamp_count_after_date_column(
        &mut self,
        comma_separated_timestamps_column_name: &str,
        date_column_name: &str,
        new_column_name: &str,
    ) -> &mut Self {
        fn parse_timestamp(time_str: &str) -> Result<NaiveDateTime, String> {
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S %p",
            ];

            let parsed_date = formats
                .iter()
                .find_map(|&format| NaiveDateTime::parse_from_str(time_str, format).ok())
                .or_else(|| {
                    DateTime::parse_from_rfc2822(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                })
                .or_else(|| {
                    DateTime::parse_from_rfc3339(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                });

            match parsed_date {
                Some(date) => Ok(date),
                None => Err(format!("Unable to parse '{}' as a timestamp", time_str)),
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Get indices of the relevant columns
        let timestamps_column_index = self
            .headers
            .iter()
            .position(|h| h == comma_separated_timestamps_column_name)
            .expect("Timestamps column not found");
        let date_column_index = self
            .headers
            .iter()
            .position(|h| h == date_column_name)
            .expect("Date column not found");

        for row in &mut self.data {
            // Parse the date and timestamps from the row
            let date_str = &row[date_column_index];
            let timestamps_str = &row[timestamps_column_index];

            if let Ok(date) = parse_timestamp(date_str) {
                let timestamps: Vec<&str> = timestamps_str.split(";").collect();
                let mut count = 0;

                for time_str in timestamps {
                    if let Ok(time) = parse_timestamp(time_str) {
                        if time > date {
                            count += 1;
                        }
                    }
                }
                row.push(count.to_string());
            } else {
                row.push("0".to_string());
            }
        }

        self
    }

    /// Appends a new column with the count of timestamps before a given date.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    ///
    /// let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "timestamps".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "2023-01-01 11:30:00;2023-01-01 11:00:00".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "2022-12-31 11:00:00;2022-12-31 10:30:00".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "2023-01-02 12:00:00;2023-01-02 11:00:00".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "28".to_string(), "2023-01-01 11:00:00".to_string(), "2023-01-01 12:00:00;2023-01-01 13:00:00".to_string()]
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    /// csv_builder.append_semi_colon_separated_timestamp_count_before_date_column("timestamps", "date", "count_before_date");
    /// ```
    pub fn append_semi_colon_separated_timestamp_count_before_date_column(
        &mut self,
        comma_separated_timestamps_column_name: &str,
        date_column_name: &str,
        new_column_name: &str,
    ) -> &mut Self {
        fn parse_timestamp(time_str: &str) -> Result<NaiveDateTime, String> {
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S %p",
            ];

            let parsed_date = formats
                .iter()
                .find_map(|&format| NaiveDateTime::parse_from_str(time_str, format).ok())
                .or_else(|| {
                    DateTime::parse_from_rfc2822(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                })
                .or_else(|| {
                    DateTime::parse_from_rfc3339(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                });

            match parsed_date {
                Some(date) => Ok(date),
                None => Err(format!("Unable to parse '{}' as a timestamp", time_str)),
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Get indices of the relevant columns
        let timestamps_column_index = self
            .headers
            .iter()
            .position(|h| h == comma_separated_timestamps_column_name)
            .expect("Timestamps column not found");
        let date_column_index = self
            .headers
            .iter()
            .position(|h| h == date_column_name)
            .expect("Date column not found");

        for row in &mut self.data {
            // Parse the date and timestamps from the row
            let date_str = &row[date_column_index];
            let timestamps_str = &row[timestamps_column_index];

            if let Ok(date) = parse_timestamp(date_str) {
                let timestamps: Vec<&str> = timestamps_str.split(";").collect();
                let mut count = 0;

                for time_str in timestamps {
                    if let Ok(time) = parse_timestamp(time_str) {
                        if time < date {
                            count += 1;
                        }
                    }
                }
                row.push(count.to_string());
            } else {
                row.push("0".to_string());
            }
        }

        self
    }

    /// Appends a new column with the updated timestamp by adding a number of days specified in another column.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    ///
    /// let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "days".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "2.5".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "1.5".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "3".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    /// csv_builder.append_added_days_column_relative_to_adjacent_column("days", "date", "new_date");
    ///
    /// assert_eq!(csv_builder.get_data().unwrap()[0][5], "2023-01-04 00:00:00");
    /// assert_eq!(csv_builder.get_data().unwrap()[1][5], "2023-01-01 23:59:59");
    /// assert_eq!(csv_builder.get_data().unwrap()[2][5], "2023-01-05 13:00:00");
    /// ```
    pub fn append_added_days_column_relative_to_adjacent_column(
        &mut self,
        days_column_name: &str,
        timestamp_column_name: &str,
        new_column_name: &str,
    ) -> &mut Self {
        fn parse_timestamp(time_str: &str) -> Result<NaiveDateTime, String> {
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S %p",
            ];

            let parsed_date = formats
                .iter()
                .find_map(|&format| NaiveDateTime::parse_from_str(time_str, format).ok())
                .or_else(|| {
                    DateTime::parse_from_rfc2822(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                })
                .or_else(|| {
                    DateTime::parse_from_rfc3339(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                });

            match parsed_date {
                Some(date) => Ok(date),
                None => Err(format!("Unable to parse '{}' as a timestamp", time_str)),
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Get index of the relevant columns
        let timestamp_column_index = self
            .headers
            .iter()
            .position(|h| h == timestamp_column_name)
            .expect("Timestamp column not found");

        let days_column_index = self
            .headers
            .iter()
            .position(|h| h == days_column_name)
            .expect("Days column not found");

        for row in &mut self.data {
            // Parse the timestamp from the row
            let timestamp_str = &row[timestamp_column_index];
            let days_str = &row[days_column_index];

            if let Ok(timestamp) = parse_timestamp(timestamp_str) {
                if let Ok(days) = days_str.parse::<f64>() {
                    let total_seconds_to_add = (days * 86400.0) as i64;
                    let new_timestamp = timestamp + ChronoDuration::seconds(total_seconds_to_add);
                    row.push(new_timestamp.format("%Y-%m-%d %H:%M:%S").to_string());
                } else {
                    row.push("Invalid Days".to_string());
                }
            } else {
                row.push("Invalid Timestamp".to_string());
            }
        }

        self
    }

    /// Appends a new column with the updated timestamp by subtracting a number of days specified in another column.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use std::collections::HashMap;
    ///
    /// let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "days".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "2.5".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "1.5".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "3".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    /// csv_builder.append_subtracted_days_column_relative_to_adjacent_column("days", "date", "new_date");
    ///
    /// assert_eq!(csv_builder.get_data().unwrap()[0][5], "2022-12-30 00:00:00");
    /// assert_eq!(csv_builder.get_data().unwrap()[1][5], "2022-12-29 23:59:59");
    /// assert_eq!(csv_builder.get_data().unwrap()[2][5], "2022-12-30 13:00:00");
    /// ```
    pub fn append_subtracted_days_column_relative_to_adjacent_column(
        &mut self,
        days_column_name: &str,
        timestamp_column_name: &str,
        new_column_name: &str,
    ) -> &mut Self {
        fn parse_timestamp(time_str: &str) -> Result<NaiveDateTime, String> {
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S %p",
            ];

            let parsed_date = formats
                .iter()
                .find_map(|&format| NaiveDateTime::parse_from_str(time_str, format).ok())
                .or_else(|| {
                    DateTime::parse_from_rfc2822(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                })
                .or_else(|| {
                    DateTime::parse_from_rfc3339(time_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                });

            match parsed_date {
                Some(date) => Ok(date),
                None => Err(format!("Unable to parse '{}' as a timestamp", time_str)),
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Get index of the relevant columns
        let timestamp_column_index = self
            .headers
            .iter()
            .position(|h| h == timestamp_column_name)
            .expect("Timestamp column not found");

        let days_column_index = self
            .headers
            .iter()
            .position(|h| h == days_column_name)
            .expect("Days column not found");

        for row in &mut self.data {
            // Parse the timestamp from the row
            let timestamp_str = &row[timestamp_column_index];
            let days_str = &row[days_column_index];

            if let Ok(timestamp) = parse_timestamp(timestamp_str) {
                if let Ok(days) = days_str.parse::<f64>() {
                    let total_seconds_to_subtract = (days * 86400.0) as i64;
                    let new_timestamp =
                        timestamp - ChronoDuration::seconds(total_seconds_to_subtract);
                    row.push(new_timestamp.format("%Y-%m-%d %H:%M:%S").to_string());
                } else {
                    row.push("Invalid Days".to_string());
                }
            } else {
                row.push("Invalid Timestamp".to_string());
            }
        }

        self
    }

    /// Appends a new column with the added timestamp by adding a specified number of days to a date column.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["date".to_string()];
    /// let data = vec![
    ///     vec!["2023-01-01 12:00:00".to_string()],
    ///     vec!["2022-12-31 11:59:59".to_string()],
    ///     vec!["2023-01-02 13:00:00".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    /// csv_builder.append_added_days_column("date", "2.5", "new_date");
    ///
    /// assert_eq!(csv_builder.get_data().unwrap()[0][1], "2023-01-04 00:00:00");
    /// assert_eq!(csv_builder.get_data().unwrap()[1][1], "2023-01-02 23:59:59");
    /// assert_eq!(csv_builder.get_data().unwrap()[2][1], "2023-01-05 01:00:00");
    /// ```
    pub fn append_added_days_column(
        &mut self,
        date_column_name: &str,
        number_of_days: &str,
        new_column_name: &str,
    ) -> &mut Self {
        fn parse_date(date_str: &str) -> Result<NaiveDateTime, String> {
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S %p",
            ];

            let parsed_date = formats
                .iter()
                .find_map(|&format| NaiveDateTime::parse_from_str(date_str, format).ok())
                .or_else(|| {
                    DateTime::parse_from_rfc2822(date_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                })
                .or_else(|| {
                    DateTime::parse_from_rfc3339(date_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                });

            match parsed_date {
                Some(date) => Ok(date),
                None => Err(format!("Unable to parse '{}' as a date", date_str)),
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Get index of the relevant column
        let date_column_index = self
            .headers
            .iter()
            .position(|h| h == date_column_name)
            .expect("Date column not found");

        for row in &mut self.data {
            let date_str = &row[date_column_index];

            if let Ok(date) = parse_date(date_str) {
                if let Ok(days) = number_of_days.parse::<f64>() {
                    let total_seconds_to_add = (days * 86400.0) as i64;
                    let new_date = date + ChronoDuration::seconds(total_seconds_to_add);
                    row.push(new_date.format("%Y-%m-%d %H:%M:%S").to_string());
                } else {
                    row.push("Invalid Days".to_string());
                }
            } else {
                row.push("Invalid Date".to_string());
            }
        }

        self
    }

    /// Appends a new column with the updated timestamp by subtracting a specified number of days from a date column.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["date".to_string()];
    /// let data = vec![
    ///     vec!["2023-01-05 12:00:00".to_string()],
    ///     vec!["2023-01-10 11:59:59".to_string()],
    ///     vec!["2023-01-02 13:00:00".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    /// csv_builder.append_subtracted_days_column("date", "2.5", "new_date");
    ///
    /// assert_eq!(csv_builder.get_data().unwrap()[0][1], "2023-01-03 00:00:00");
    /// assert_eq!(csv_builder.get_data().unwrap()[1][1], "2023-01-07 23:59:59");
    /// assert_eq!(csv_builder.get_data().unwrap()[2][1], "2022-12-31 01:00:00");
    /// ```
    pub fn append_subtracted_days_column(
        &mut self,
        date_column_name: &str,
        number_of_days: &str,
        new_column_name: &str,
    ) -> &mut Self {
        fn parse_date(date_str: &str) -> Result<NaiveDateTime, String> {
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S %p",
            ];

            let parsed_date = formats
                .iter()
                .find_map(|&format| NaiveDateTime::parse_from_str(date_str, format).ok())
                .or_else(|| {
                    DateTime::parse_from_rfc2822(date_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                })
                .or_else(|| {
                    DateTime::parse_from_rfc3339(date_str)
                        .map(|dt| dt.naive_local())
                        .ok()
                });

            match parsed_date {
                Some(date) => Ok(date),
                None => Err(format!("Unable to parse '{}' as a date", date_str)),
            }
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Get index of the relevant column
        let date_column_index = self
            .headers
            .iter()
            .position(|h| h == date_column_name)
            .expect("Date column not found");

        for row in &mut self.data {
            let date_str = &row[date_column_index];

            if let Ok(date) = parse_date(date_str) {
                if let Ok(days) = number_of_days.parse::<f64>() {
                    let total_seconds_to_subtract = (days * 86400.0) as i64;
                    let new_date = date - ChronoDuration::seconds(total_seconds_to_subtract);
                    row.push(new_date.format("%Y-%m-%d %H:%M:%S").to_string());
                } else {
                    row.push("Invalid Days".to_string());
                }
            } else {
                row.push("Invalid Date".to_string());
            }
        }

        self
    }

    /// Appends a new column with the difference in days between two existing columns.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["A".to_string(), "B".to_string()];
    /// let data = vec![
    ///     vec!["2023-01-05".to_string(), "2023-01-07".to_string()],
    ///     vec!["2023-01-10".to_string(), "2023-01-05".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    /// csv_builder.append_day_difference_column("A", "B", "difference");
    ///
    /// assert_eq!(csv_builder.get_data().unwrap()[0][2], "-2.00");
    /// assert_eq!(csv_builder.get_data().unwrap()[1][2], "5.00");
    /// ```
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let headers = vec!["A".to_string(), "B".to_string()];
    /// let data = vec![
    ///     vec!["2023-01-05 12:00:00".to_string(), "2023-01-01 12:30:00".to_string()],
    ///     vec!["2023-01-10 12:00:00".to_string(), "2023-01-05 12:00:00".to_string()],
    /// ];
    ///
    /// let mut csv_builder = CsvBuilder::from_raw_data(headers, data);
    /// csv_builder.append_day_difference_column("A", "B", "difference");
    ///
    /// assert_eq!(csv_builder.get_data().unwrap()[0][2], "3.98");
    /// assert_eq!(csv_builder.get_data().unwrap()[1][2], "5.00");
    /// ```
    pub fn append_day_difference_column(
        &mut self,
        column_a: &str,
        column_b: &str,
        new_column_name: &str,
    ) -> &mut Self {
        fn parse_date(date_str: &str) -> Result<NaiveDateTime, String> {
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S %p",
            ];

            for format in formats {
                if format == "%Y-%m-%d" {
                    if let Ok(date) = NaiveDateTime::parse_from_str(
                        &(date_str.to_string() + " 00:00:00"),
                        "%Y-%m-%d %H:%M:%S",
                    ) {
                        return Ok(date);
                    }
                } else if let Ok(date) = NaiveDateTime::parse_from_str(date_str, format) {
                    return Ok(date);
                }
            }

            if let Ok(dt) = DateTime::parse_from_rfc2822(date_str) {
                return Ok(dt.naive_local());
            }
            if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
                return Ok(dt.naive_local());
            }

            Err(format!("Unable to parse '{}' as a date", date_str))
        }

        // Add new column header
        self.headers.push(new_column_name.to_string());

        // Get index of the relevant columns
        let column_a_index = self
            .headers
            .iter()
            .position(|h| h == column_a)
            .expect("Column A not found");
        let column_b_index = self
            .headers
            .iter()
            .position(|h| h == column_b)
            .expect("Column B not found");

        for row in &mut self.data {
            let date_a_str = &row[column_a_index];
            let date_b_str = &row[column_b_index];

            if let Ok(date_a) = parse_date(date_a_str) {
                if let Ok(date_b) = parse_date(date_b_str) {
                    let duration = date_a.signed_duration_since(date_b);
                    let days_difference =
                        (duration.num_seconds() as f64 / 86400.0 * 100.0).round() / 100.0;
                    row.push(format!("{:.2}", days_difference));
                } else {
                    row.push("Invalid Date B".to_string());
                }
            } else {
                row.push("Invalid Date A".to_string());
            }
        }

        self
    }

    /// Appends a clustering column
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::clustering_utils::ClusteringConfig;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    /// use std::env::current_dir;
    ///
    ///
    /// let headers = vec![
    ///     "customer_id".to_string(),
    ///     "age".to_string(),
    ///     "annual_income".to_string(),
    ///     "spending_score".to_string(),
    /// ];
    ///
    /// let data = vec![
    ///     vec!["1".to_string(), "19".to_string(), "15".to_string(), "39".to_string()],
    ///     vec!["2".to_string(), "21".to_string(), "15".to_string(), "81".to_string()],
    ///     vec!["3".to_string(), "20".to_string(), "16".to_string(), "6".to_string()],
    ///     vec!["4".to_string(), "23".to_string(), "16".to_string(), "77".to_string()],
    ///     vec!["5".to_string(), "31".to_string(), "17".to_string(), "40".to_string()],
    ///     vec!["6".to_string(), "22".to_string(), "17".to_string(), "76".to_string()],
    ///     vec!["7".to_string(), "35".to_string(), "18".to_string(), "6".to_string()],
    ///     vec!["8".to_string(), "23".to_string(), "18".to_string(), "94".to_string()],
    ///     vec!["9".to_string(), "64".to_string(), "19".to_string(), "3".to_string()],
    ///     vec!["10".to_string(), "30".to_string(), "19".to_string(), "72".to_string()],
    /// ];
    ///
    /// let mut builder = CsvBuilder::from_raw_data(headers, data);

    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///     let param_column_names = "age, annual_income, spending_score";
    ///     let cluster_column_name = "CLUSTERING";
    ///  
    ///     let clustering_config = ClusteringConfig {
    ///         operation: "KMEANS".to_string(),
    ///         optimal_n_cluster_finding_method: "ELBOW".to_string(),
    ///         dbscan_eps: "".to_string(),
    ///         dbscan_min_samples: "".to_string(),
    ///     };
    ///
    ///     builder.append_clustering_column(param_column_names, cluster_column_name, clustering_config).await;
    ///     // Use get_headers and get_data methods to access private fields
    ///     let updated_headers = builder.get_headers().unwrap_or(&[]).to_vec();
    ///     let updated_data = builder.get_data().unwrap_or(&vec![]).clone();
    ///
    ///     println!("Updated Builder Headers: {:?}", updated_headers);
    ///     println!("Updated Builder Data: {:?}", updated_data);
    ///
    ///     assert_eq!(updated_headers, vec!["customer_id", "age", "annual_income", "spending_score", "CLUSTERING"]);
    ///     assert_eq!(updated_data.len(), 10);
    /// });
    /// ```

    pub async fn append_clustering_column(
        &mut self,
        param_column_names: &str,
        cluster_column_name: &str,
        clustering_config: ClusteringConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        // Split and trim column names into a vector
        let param_columns: Vec<&str> = param_column_names.split(',').map(|s| s.trim()).collect();

        // Create a copy of the builder
        let mut temp_builder = self.from_copy();

        // Retain only the parameter columns
        let columns_to_retain: Vec<&str> = {
            let columns = param_columns.clone();
            columns
        };
        temp_builder.retain_columns(columns_to_retain);

        // Create a temporary file
        if let Ok(mut temp_file) = NamedTempFile::new() {
            /*
            // Write headers to the temporary file
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            for row in &temp_builder.data {
                let _ = writeln!(temp_file, "{}", row.join(","));
            }
            */

            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                if row.len() == header_len {
                    let escaped_row = row
                        .iter()
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row);
                    continue;
                }
            }

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                //println!("Temporary CSV file contents:");

                // Call the predict method from XgbConnect
                if let Ok(result) = ClusteringConnect::cluster(
                    csv_path_str,
                    param_column_names,
                    cluster_column_name,
                    clustering_config,
                )
                .await
                {
                    //dbg!(&result);

                    let (headers, rows) = result;

                    // Find the index of the prediction column in the result
                    let cluster_idx = headers
                        .iter()
                        .position(|h| h == cluster_column_name)
                        .unwrap();

                    // Extract the prediction values from the rows
                    let cluster_values: Vec<String> =
                        rows.iter().map(|row| row[cluster_idx].clone()).collect();

                    // Append the prediction column to the original data
                    if let Some(cluster_header_idx) =
                        self.headers.iter().position(|h| h == cluster_column_name)
                    {
                        // Update existing prediction column
                        for (i, value) in cluster_values.iter().enumerate() {
                            self.data[i][cluster_header_idx] = value.clone();
                        }
                    } else {
                        // Add a new prediction column
                        self.headers.push(cluster_column_name.to_string());
                        for (i, value) in cluster_values.iter().enumerate() {
                            self.data[i].push(value.clone());
                        }
                    }

                    // Clone the headers to avoid mutable and immutable borrow conflict
                    let cloned_headers = self.headers.clone();

                    // Order columns in the original order with the new column appended
                    self.order_columns(cloned_headers.iter().map(|s| s.as_str()).collect());

                    // Return the updated self
                    return self;
                }
                // }
            }
        }

        self
    }
}
