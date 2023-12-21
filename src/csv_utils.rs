// csv_utils.rs
use crate::df_utils::DataFrame;
use chrono::{DateTime, NaiveDateTime};
use csv::Writer;
use serde_json::Value;
use futures::executor::block_on;
use futures::future::join_all;
use futures::Future;
use fuzzywuzzy::fuzz;
use regex::Regex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

/// A utility struct for converting JSON data to CSV format.
pub struct CsvConverter;

impl CsvConverter {
    pub fn from_json(json_data: &str, file_path: &str) -> Result<(), Box<dyn Error>> {
        /// Parses the JSON string into serde_json::Value
        let data: Value = serde_json::from_str(json_data)?;

        /// Initializes a writer to write to the specified file
        let file = File::create(file_path)?;
        let mut wtr = Writer::from_writer(file);

        /// Checks if the top-level JSON structure is an array
        if let Value::Array(items) = data {
            // Process only if there are items in the array
            if let Some(first_item) = items.first() {
                if let Value::Object(map) = first_item {
                    // Write the headers (keys of the first item)
                    wtr.write_record(map.keys())?;
                }

                // Write the rest of the data
                for item in items {
                    if let Value::Object(map) = item {
                        let row: Vec<String> = map.values().map(|v| v.to_string()).collect();
                        wtr.write_record(&row)?;
                    }
                }
            }
        }

        // Flush and finish writing to the file
        wtr.flush()?;
        Ok(())
    }
}

/// Defines the trait for comparison values
pub trait CompareValue {
    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool;
}

/// Implements CompareValue for a single string reference
impl CompareValue for &str {
    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool {
        // Simplified example, implement the logic as per your requirement
        match compare_as {
            "COMPARE_AS_TEXT" => match operation {
                "==" => cell_value == *self,
                "CONTAINS" => cell_value.contains(*self),
                "STARTS_WITH" => cell_value.starts_with(*self),
                _ => false,
            },

            "COMPARE_AS_NUMBERS" => match (cell_value.parse::<f64>(), self.parse::<f64>()) {
                (Ok(n1), Ok(n2)) => match operation {
                    "==" => n1 == n2,
                    ">" => n1 > n2,
                    "<" => n1 < n2,
                    _ => false,
                },
                _ => {
                    println!("Failed to parse as numbers: '{}' or '{}'", cell_value, self);
                    false
                }
            },

            "COMPARE_AS_TIMESTAMPS" => {
                let parsed_row_value = CsvBuilder::parse_timestamp(cell_value);
                let parsed_compare_value = CsvBuilder::parse_timestamp(self);

                match (parsed_row_value, parsed_compare_value) {
                    (Ok(row_date), Ok(compare_date)) => match operation {
                        "==" => row_date == compare_date,
                        ">" => row_date > compare_date,
                        "<" => row_date < compare_date,
                        _ => false,
                    },
                    _ => {
                        println!("Error comparing timestamps. Unable to parse '{}' or '{}' as timestamps.", cell_value, self);
                        false
                    }
                }
            }
            _ => false,
        }
    }
}

/// Implements CompareValue for a Vec<&str> reference
impl CompareValue for Vec<&str> {
    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool {
        if operation.starts_with("FUZZ_MIN_SCORE_") && compare_as == "COMPARE_AS_TEXT" {
            // Extract the score threshold from the operation string
            let score_threshold: i32 = operation["FUZZ_MIN_SCORE_".len()..].parse().unwrap_or(70);

            // dbg!(&score_threshold);

            let re = Regex::new(r"[^a-zA-Z\s]").unwrap();

            // Replace non-alphabet characters with nothing (effectively removing them)
            let only_alpha = re.replace_all(cell_value, "");

            // Trim, split on whitespace, and join to ensure single spaces between words
            let cleaned_cell_value = only_alpha
                .trim()
                .split_whitespace()
                .collect::<Vec<&str>>()
                .join(" ");
            let words: Vec<&str> = cleaned_cell_value.split_whitespace().collect();
            let mut all_scores = vec![];

            for &value in self.iter() {
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
            i32::from(max_score) >= score_threshold
        } else {
            false
        }
    }
}

/// A flexible builder for creating and writing to CSV files.
///
/// This struct allows for a fluent interface to build and write to a CSV file,
/// supporting method chaining. It uses a generic writer to handle different types
/// of outputs, primarily working with file-based writing.
pub struct CsvBuilder {
    headers: Vec<String>,
    data: Vec<Vec<String>>,
    limit: Option<usize>,
    error: Option<Box<dyn Error>>,
}

impl CsvBuilder {
    /// A function to get available options and their syntax
    pub fn get_options(&mut self) -> &mut Self {
        let mut options = [
            ".save_as('/path/to/your/file.csv')",
            ".set_header(&['Column1', 'Column2', 'Column3']) // Only on CsvBuilder::new() instantiations",
            ".add_row(&['Row1-1', 'Row1-2', 'Row1-3'])", 
            ".add_rows(&[&['Row1-1', 'Row1-2', 'Row1-3'], &['Row2-1', 'Row2-2', 'Row2-3']])",
            ".order_columns(vec!['Column1', '...', 'Column5', 'Column2'])",
            ".order_columns(vec!['...', 'Column5', 'Column2'])",
            ".order_columns(vec!['Column1', 'Column5', '...'])",
            ".print_columns()",
            ".print_row_count()",
            ".print_first_row()",
            ".print_last_row()",
            ".print_rows_range(2,5)",
            ".print_rows()",
            ".cascade_sort(vec![('Column1', 'DESC'), ('Column3', 'ASC')])",
            ".drop_columns(vec!['Column1', 'Column3'])",
            ".rename_columns(vec![('Column1', 'NewColumn1'), ('Column3', 'NewColumn3')])",
            ".where_('column1', '==', '42', 'COMPARE_AS_NUMBERS')",
            ".where_('column1', '==', 'hello', 'COMPARE_AS_TEXT')",
            ".where_('column1', 'CONTAINS', 'apples', 'COMPARE_AS_TEXT')",
            ".where_('column1', 'STARTS_WITH', 'discounted', 'COMPARE_AS_TEXT')",
            ".where_('stated_locality_address','FUZZ_MIN_SCORE_90',vec!['Shastri park','kamal nagar'], 'COMPARE_AS_TEXT') // Adjust score value to any two digit number like FUZZ_MIN_SCORE_23, FUZZ_MIN_SCORE_67, etc.",
            ".where_('column1', '>', '23-01-01', 'COMPARE_AS_TIMESTAMPS')",
            ".limit(10)"
        ];
        options.sort();

        println!();
        println!("Available CsvBuilder chain options:");
        println!();

        for option in &options {
            let option_with_double_quotes = option.replace("'", "\"");
            println!("  - {}", option_with_double_quotes);
        }

        self

    }

    /// Creates a new `CsvBuilder` instance with empty headers and data.
    ///
    /// # Example
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new();
    /// ```
    ///
    /// Creates an empty `CsvBuilder` instance that can be used to build a CSV file.
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
    /// # Example
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let result = CsvBuilder::from_csv("/path/to/your/file.csv")
    ///     .add_row(&["Row1-1", "Row1-2", "Row1-3"])
    ///     .add_rows(&[&["Row2-1", "Row2-2", "Row2-3"], &["Row3-1", "Row3-2", "Row3-3"]])
    ///     .save_as("/path/to/your/file.csv");
    /// ```
    ///
    /// Creates a `CsvBuilder` from an existing CSV file, sets headers, and adds rows using method chaining.

    pub fn from_csv(file_path: &str) -> Self {
        let mut builder = CsvBuilder::new();

        match File::open(file_path) {
            Ok(file) => {
                let mut rdr = csv::Reader::from_reader(file);

                // Read the headers
                if let Ok(hdrs) = rdr.headers() {
                    builder.headers = hdrs.iter().map(String::from).collect();
                }

                // Read the data rows
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

    /// Creates a `CsvBuilder` from a DataFrame, extracting headers and data.
    ///
    /// # Example
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::df_utils::DataFrame; // Replace with actual DataFrame type
    ///
    /// let data_frame = // Your DataFrame initialization here
    /// let builder = CsvBuilder::from_dataframe(data_frame)
    ///     .add_row(&["Row1-1", "Row1-2", "Row1-3"])
    ///     .add_rows(&[&["Row2-1", "Row2-2", "Row2-3"], &["Row3-1", "Row3-2", "Row3-3"]])
    ///     .save_as("/path/to/your/file.csv");
    /// ```
    pub fn from_dataframe(data_frame: DataFrame) -> Self {
        let mut builder = CsvBuilder::new();

        if let Some(first_record) = data_frame.first() {
            // Extract headers in the order they appear in the first record
            let headers: Vec<String> = first_record.keys().cloned().collect();
            builder.headers = headers.clone(); // Store headers separately

            // Iterate over records to create data rows
            for record in data_frame {
                let mut row = Vec::new();
                for header in &headers {
                    let value = record.get(header).map_or("".to_string(), |v| v.to_string());
                    row.push(value);
                }
                builder.data.push(row); // Add row to data
            }
        }

        builder
    }

    /// Saves data in the `CsvBuilder` to a new CSV file at `new_file_path`.
    pub fn save_as(&mut self, new_file_path: &str) -> Result<(), Box<dyn Error>> {
        match File::create(new_file_path) {
            Ok(file) => {
                let mut wtr = Writer::from_writer(file);

                for record in &self.data {
                    wtr.write_record(record)?;
                }

                wtr.flush()?;
            }
            Err(e) => return Err(Box::new(e)),
        }

        Ok(())
    }

    /// Sets the CSV header using an array of strings.
    pub fn set_header(mut self, header: &[&str]) -> Self {
        if self.error.is_none() {
            let header_row = header.iter().map(|s| s.to_string()).collect();
            self.data.insert(0, header_row);
        }
        self
    }

    /// Adds a data row to the CSV.
    pub fn add_row(mut self, row: &[&str]) -> Self {
        if self.error.is_none() {
            let row_vec = row.iter().map(|s| s.to_string()).collect();
            self.data.push(row_vec);
        }
        self
    }

    /// Adds multiple data rows to the CSV.
    pub fn add_rows(mut self, rows: &[&[&str]]) -> Self {
        if self.error.is_none() {
            for row in rows {
                let row_vec = row.iter().map(|s| s.to_string()).collect();
                self.data.push(row_vec);
            }
        }
        self
    }

    /// In CSV column order manipulation, the `...` symbol acts as a pivot point to specify
    /// where specified columns should be placed within the reordered column sequence.
    ///
    /// This flexibility allows you to control column placement in various scenarios, such as
    /// moving columns to the start, end, or between existing columns.
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

        // Reorder all rows based on the new header
        let reordered_data = self
            .data
            .iter()
            .map(|row| {
                reordered_header
                    .iter() // Use cloned reordered_header
                    .map(|col_name| {
                        let col_name_str = col_name.as_str();
                        row[column_map[col_name_str]].clone()
                    })
                    .collect()
            })
            .collect::<Vec<Vec<String>>>();

        // Prepend the new header row to the data
        let mut new_data = Vec::new();
        new_data.push(reordered_header);
        new_data.extend(reordered_data);

        // Update self.data with the reordered data
        self.data = new_data;

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
    pub fn print_first_row(&mut self) -> &mut Self {
        println!();
        if let Some(first_row) = self.data.first() {
            println!("First row:");
            self.print_row_json(first_row);
        } else {
            println!("CSV is empty.");
        }
        self
    }

    /// Prints the last row of the CSV data.
    pub fn print_last_row(&mut self) -> &mut Self {
        println!();
        if let Some(last_row) = self.data.last() {
            println!("Last row:");
            self.print_row_json(last_row);
        } else {
            println!("CSV is empty.");
        }
        self
    }

    /// Prints rows within a specified range from the CSV data.
    pub fn print_rows_range(&mut self, start: usize, end: usize) -> &mut Self {
        let rows = self.data.get(start..end).unwrap_or(&[]);
        println!();
        for (offset, row) in rows.iter().enumerate() {
            let index = start + offset; // Adjusting the index
            println!("Row {}: ", index);
            self.print_row_json(row);
        }
        self
    }

    /// Prints all rows of the CSV data.
    pub fn print_rows(&mut self) -> &mut Self {
        println!();
        for (index, row) in self.data.iter().enumerate() {
            println!("Row {}: ", index);
            self.print_row_json(row);
        }

        // Print the total count of rows
        println!("\nTotal rows: {}", self.data.len());
        self
    }

    /// Sorts the CSV data based on specified column orders.
    pub fn cascade_sort<'a>(&'a mut self, orders: Vec<(&'a str, &'a str)>) -> &'a mut Self {
        // pub fn cascade_sort(mut self, orders: Vec<(&str, &str)>) -> &mut Self {
        if let Some(header_row) = self.data.first().cloned() {
            // Clone the header row
            let column_indices: HashMap<&str, usize> = header_row
                .iter()
                .enumerate()
                .map(|(i, name)| (name.as_str(), i))
                .collect();

            // Now sort the data starting from the second row
            self.data[1..].sort_by(|a, b| {
                let mut cmp = std::cmp::Ordering::Equal;
                for (column_name, order) in &orders {
                    if let Some(&index) = column_indices.get(column_name) {
                        cmp = if *order == "ASC" {
                            a[index].cmp(&b[index])
                        } else {
                            // Assume DESC if not ASC
                            b[index].cmp(&a[index])
                        };
                        if cmp != std::cmp::Ordering::Equal {
                            break;
                        }
                    }
                }
                cmp
            });
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

    /// Filters the rows based on a column name, condition, value, and comparison type.
    pub fn where_<T: CompareValue>(
        &mut self,
        column_name: &str,
        operation: &str,
        value: T, // Accepts any type that implements CompareValue
        compare_as: &str,
    ) -> &mut Self {
        if let Some(column_index) = self.headers.iter().position(|h| h == column_name) {
            let original_data = std::mem::replace(&mut self.data, Vec::new());

            let filtered_data = original_data
                .into_iter()
                .filter(|row| {
                    if let Some(cell_value) = row.get(column_index) {
                        value.apply(cell_value, operation, compare_as)
                    } else {
                        false
                    }
                })
                .collect();

            self.data = filtered_data;
        } else {
            println!("Column '{}' not found in headers.", column_name);
        }
        self
    }

    /// Helper function to parse timestamps
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

    /// Sets a limit on the number of rows to be included in the CSV and truncates the data if it exceeds the limit.
    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);

        // Truncate the data vector if it exceeds the limit
        if self.data.len() > limit {
            self.data.truncate(limit);
        }

        self
    }
}

/// Represents a caching mechanism for CSV results, holding a data generator, cache path, and cache duration.
pub struct CsvResultCacher {
    data_generator: Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>>>> + Send + Sync>,
    cache_path: String, // Still using String here to store the path
    cache_duration: Duration,
}

impl CsvResultCacher {

    /// Constructs a new `CsvResultCacher` with a specified data generator, cache path, and cache duration in minutes.
    pub fn new<F>(data_generator: F, cache_path: String, cache_duration_minutes: u64) -> Self
    where
        F: Fn() -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>>>> + Send + Sync + 'static,
    {
        CsvResultCacher {
            data_generator: Box::new(data_generator),
            cache_path,
            cache_duration: Duration::from_secs(cache_duration_minutes * 60),
        }
    }

    /// Asynchronously fetches CSV data using a given generator, cache path, and cache duration, and initializes the cacher.
    pub async fn fetch_async<F>(
        data_generator: F,
        cache_path: &str,
        cache_duration_minutes: u64,
    ) -> Result<(), Box<dyn Error>>
    where
        F: Fn() -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>>>> + Send + Sync + 'static,
    {
        let cacher = CsvResultCacher::new(
            data_generator, 
            cache_path.to_string(), // Convert &str to String here
            cache_duration_minutes,
        );
        cacher.fetch_data().await
    }

    /// Checks if the cached data is still valid based on the current time and the cache duration.
    fn is_cache_valid(&self) -> bool {
        if let Ok(metadata) = fs::metadata(&self.cache_path) {
            if let Ok(modified) = metadata.modified() {
                return SystemTime::now()
                    .duration_since(modified)
                    .map(|duration| duration < self.cache_duration)
                    .unwrap_or(false);
            }
        }
        false
    }

    /// Asynchronously fetches data, using cached data if valid, or generating new data otherwise.
    pub async fn fetch_data(&self) -> Result<(), Box<dyn Error>> {
        if self.is_cache_valid() {
            println!("Using cached CSV file at {}", &self.cache_path);
            // Optionally, add logic to read and process the cached CSV file
        } else {
            println!("Generating new CSV file.");
            (self.data_generator)().await?;
            // Implement logic to save the generated data to self.cache_path
        }
        Ok(())
    }
}
