// csv_utils.rs
use crate::db_utils::DbConnect;
use calamine::{open_workbook, Reader, Xls};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};
use csv::Writer;
use futures::executor::block_on;
use futures::future::join_all;
use futures::Future;
use fuzzywuzzy::fuzz;
use rand::{seq::SliceRandom, thread_rng};
use regex::Regex;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

/// A utility struct for converting JSON data to CSV format.
pub struct CsvConverter;

impl CsvConverter {
    pub fn from_json(json_data: &str, file_path: &str) -> Result<(), Box<dyn Error>> {
        let data: Value = serde_json::from_str(json_data)?;

        let file = File::create(file_path)?;
        let mut wtr = Writer::from_writer(file);

        if let Value::Array(items) = data {
            if let Some(first_item) = items.first() {
                if let Value::Object(map) = first_item {
                    wtr.write_record(map.keys())?;
                }

                for item in items {
                    if let Value::Object(map) = item {
                        let row: Vec<String> = map
                            .values()
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                _ => v.to_string(),
                            })
                            .collect();
                        wtr.write_record(&row)?;
                    }
                }
            }
        }

        wtr.flush()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Train {
    pub input: String,
    pub output: String,
}

#[derive(Debug, Clone)]
pub struct Exp {
    pub column: String,
    pub operator: String,
    pub compare_with: ExpVal,
    pub compare_as: String,
}

#[derive(Debug, Clone)]
pub enum ExpVal {
    STR(String),
    VEC(Vec<String>),
}

/// Defines the trait for comparison values
pub trait CompareValue {
    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool;
}

/// Implements CompareValue for a single string reference
impl CompareValue for String {
    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool {
        match compare_as {
            "TEXT" => match operation {
                "==" => cell_value == *self,
                "!=" => cell_value != *self,
                "CONTAINS" => cell_value.contains(&*self),
                "STARTS_WITH" => cell_value.starts_with(&*self),
                "DOES_NOT_CONTAIN" => !cell_value.contains(&*self),
                _ => false,
            },

            "NUMBERS" => {
                // Check if cell_value is empty and provide a default value if needed
                let cell_value = if cell_value.trim().is_empty() {
                    "0"
                } else {
                    &cell_value
                };

                // Attempt to parse both cell_value and self as f64
                match (cell_value.parse::<f64>(), self.parse::<f64>()) {
                    // If both are successfully parsed, proceed with comparison
                    (Ok(n1), Ok(n2)) => match operation {
                        "==" => n1 == n2,
                        ">" => n1 > n2,
                        "<" => n1 < n2,
                        ">=" => n1 >= n2,
                        "<=" => n1 <= n2,
                        "!=" => n1 != n2,
                        _ => {
                            println!("Unexpected operation: '{}'", operation);
                            false
                        }
                    },
                    // If parsing fails for either, print an error message
                    _ => {
                        println!("Failed to parse as numbers: '{}' or '{}'", cell_value, self);
                        false
                    }
                }
            }

            "TIMESTAMPS" => {
                let parsed_row_value = CsvBuilder::parse_timestamp(cell_value);
                let parsed_compare_value = CsvBuilder::parse_timestamp(self);

                match (parsed_row_value, parsed_compare_value) {
                    (Ok(row_date), Ok(compare_date)) => match operation {
                        "==" => row_date == compare_date,
                        ">" => row_date > compare_date,
                        "<" => row_date < compare_date,
                        ">=" => row_date >= compare_date,
                        "<=" => row_date <= compare_date,
                        "!=" => row_date != compare_date,
                        _ => {
                            println!("Unexpected operation: '{}'", operation);
                            false
                        }
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
impl CompareValue for Vec<String> {
    fn apply(&self, cell_value: &str, operation: &str, compare_as: &str) -> bool {
        if operation.starts_with("FUZZ_MIN_SCORE_") && compare_as == "TEXT" {
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

pub struct Piv {
    pub index_at: &'static str,
    pub values_from: &'static str,
    pub operation: &'static str,
    pub seggregate_by: Vec<(&'static str, &'static str)>,
}

pub struct CalibConfig {
    pub header_is_at_row: &'static str,
    pub rows_range_from: (&'static str, &'static str),
}

/// A flexible builder for creating and writing to CSV files.
///
/// This struct allows for a fluent interface to build and write to a CSV file,
/// supporting method chaining. It uses a generic writer to handle different types
/// of outputs, primarily working with file-based writing.
#[derive(Debug)]
pub struct CsvBuilder {
    headers: Vec<String>,
    data: Vec<Vec<String>>,
    limit: Option<usize>,
    error: Option<Box<dyn Error>>,
}

impl CsvBuilder {
    /// Creates a new `CsvBuilder` instance with empty headers and data.
    pub fn new() -> Self {
        CsvBuilder {
            headers: Vec::new(),
            data: Vec::new(),
            limit: None,
            error: None,
        }
    }

    /// Reads data from a CSV file at the specified `file_path` and returns a `CsvBuilder`.
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

    /// Reads data from a specified sheet (by index) of an XLS file at the specified `file_path`, then returns a `CsvBuilder`.
    pub fn from_xls(file_path: &str, sheet_index: usize) -> Self {
        let mut builder = CsvBuilder::new();

        match open_workbook::<Xls<_>, _>(file_path) {
            Ok(mut workbook) => {
                let sheet_names = workbook.sheet_names();
                if sheet_index == 0 || sheet_index > sheet_names.len() {
                    // Now using IoError instead of Error
                    let error = IoError::new(ErrorKind::InvalidInput, "Sheet index out of range");
                    builder.error = Some(Box::new(error) as Box<dyn Error>);
                } else {
                    let sheet_name = &sheet_names[sheet_index - 1];
                    match workbook.worksheet_range(sheet_name) {
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

    /// Creates a `CsvBuilder` instance from headers and data.
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

    /// Creates a `CsvBuilder` instance directly from an MSSQL query.
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
        let end_index = match config.rows_range_from.1 {
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
        // Adjust the start index to align with internal zero-based indexing
        let adjusted_start = start.saturating_sub(1);
        // No need to adjust the end index as the range is exclusive

        let rows = self.data.get(adjusted_start..end).unwrap_or(&[]);
        println!();
        for (offset, row) in rows.iter().enumerate() {
            // Adjust the index for display as one-based
            let display_index = adjusted_start + offset + 1;
            println!("Row {}: ", display_index);
            self.print_row_json(row);
        }
        self
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

    /// Prints an abbreviated table of the CSV data with lines and consistent spacing for cells.
    pub fn print_table(&mut self) -> &mut Self {
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

        // Print function for rows
        let print_row = |row: &Vec<String>, max_lengths: &Vec<usize>, headers_count: usize| {
            let mut row_to_print = Vec::new();
            for (i, cell) in row.iter().enumerate() {
                if i < 4 || i >= headers_count - 4 {
                    // Adjust indices based on your headers' logic
                    row_to_print.push(cell.clone());
                }
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

    /// Aesthetically prints the frequency of all unique values in the indicated columns, sorted by frequency.
    pub fn print_freq(&mut self, columns: Vec<&str>) -> &mut Self {
        let mut column_indices = Vec::new();

        // Find the indices of the specified columns
        for col in &columns {
            if let Some(index) = self.headers.iter().position(|r| r == col) {
                column_indices.push(index);
            } else {
                println!("Column '{}' not found.", col);
            }
        }

        for &col_idx in &column_indices {
            let mut freq_map: HashMap<String, usize> = HashMap::new();

            // Count the frequency of each unique value
            for row in &self.data {
                if let Some(value) = row.get(col_idx) {
                    *freq_map.entry(value.clone()).or_insert(0) += 1;
                }
            }

            // Sorting the frequency map
            let mut sorted_freq: Vec<(String, usize)> = freq_map.into_iter().collect();
            sorted_freq.sort_by(|a, b| b.1.cmp(&a.1));

            // Print the frequencies
            println!("\nFrequency for column '{}':", self.headers[col_idx]);
            for (value, count) in sorted_freq {
                println!("{}: {}", value, count);
            }
        }

        self
    }

    /// Same as print_freq with the additional ability to cluster data
    pub fn print_freq_mapped(
        &mut self,
        columns_with_groupings: Vec<(&str, Vec<(&str, Vec<&str>)>)>,
    ) -> &mut Self {
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

            // Print the frequencies
            println!("\nFrequency for column '{}':", self.headers[col_idx]);
            for (value, count) in sorted_freq {
                println!("{}: {}", value, count);
            }
        }

        self
    }

    pub fn cascade_sort<'a>(&'a mut self, orders: Vec<(&'a str, &'a str)>) -> &'a mut Self {
        //println!("cascade_sort called");

        if let Some(_header_row) = self.data.first().cloned() {
            //println!("Header row: {:?}", header_row);

            // Assuming `self.headers` contains the correct column names
            let column_indices: HashMap<&str, usize> = self
                .headers
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    //println!("Column name found: '{}'", name); // Debug print
                    (name.as_str(), i)
                })
                .collect();

            self.data[0..].sort_by(|a, b| {
                let mut cmp = Ordering::Equal;
                for (column_name, order) in &orders {
                    if let Some(&index) = column_indices.get(column_name) {
                        let a_val = &a[index];
                        let b_val = &b[index];

                        cmp = if let (Ok(a_num), Ok(b_num)) =
                            (a_val.parse::<f64>(), b_val.parse::<f64>())
                        {
                            // Both values are numbers, compare as f64
                            if order == &"ASC" {
                                a_num.partial_cmp(&b_num).unwrap_or(Ordering::Equal)
                            } else {
                                b_num.partial_cmp(&a_num).unwrap_or(Ordering::Equal)
                            }
                        } else {
                            // At least one value is not a number, compare as string
                            if order == &"ASC" {
                                a_val.cmp(b_val)
                            } else {
                                b_val.cmp(a_val)
                            }
                        };

                        if cmp != Ordering::Equal {
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
                self.evaluate_result_expression(&expr_results, result_expression)
            })
            .count();

        // Print the count
        println!("Count: {}", count);

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

    /// Returns a HashMap where keys are column names and values are vectors of sorted (value, frequency) pairs.
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
            sorted_freq.sort_by(|a, b| b.1.cmp(&a.1));

            let column_name = &self.headers[col_idx];
            results.insert(column_name.clone(), sorted_freq);
        }

        results
    }

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
        columns: Vec<&str>,
        replacements: Vec<(&str, &str)>,
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
                    for (from, to) in &replacements {
                        *item = item.replace(*from, *to);
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

    /// Performs a flexible union operation with the data from another CSV file. This reads data from another CSV file, combines it with the current data (aligning common headers), appends non-common headers, and returns a reference to the modified `CsvBuilder`.
    pub fn set_union_with(&mut self, file_path: &str, union_type: &str) -> &mut Self {
        let mut temp_builder = CsvBuilder::from_csv(file_path);

        if let Some(error) = temp_builder.error {
            self.error = Some(error);
            return self;
        }

        // Identify unique headers
        let unique_headers_temp: Vec<String> = temp_builder
            .headers
            .iter()
            .filter(|h| !self.headers.contains(h))
            .cloned()
            .collect();

        // Update the headers of the current builder
        for header in &unique_headers_temp {
            self.headers.push(header.to_string());
        }

        // Now create common_headers after mutating self.headers
        //let _common_headers: HashSet<&String> = self.headers.iter().collect();

        // Map headers to indices for the new file
        let header_indices: HashMap<_, _> = temp_builder
            .headers
            .iter()
            .enumerate()
            .map(|(i, h)| (h.clone(), i))
            .collect();

        // Determine union operation type
        match union_type {
            "UNION_TYPE:ALL" => {
                // Simple merge - Just append all rows
                self.data.append(&mut temp_builder.data);
            }
            "UNION_TYPE:ALL_WITHOUT_DUPLICATES" => {
                // Merge without duplicates
                let mut existing_rows: HashSet<Vec<String>> = self.data.drain(..).collect();

                for row in temp_builder.data {
                    let mut aligned_row = vec![String::new(); self.headers.len()];

                    // Fill in data for common headers
                    for (i, header) in self.headers.iter().enumerate() {
                        if let Some(&index) = header_indices.get(header) {
                            if let Some(value) = row.get(index) {
                                aligned_row[i] = value.clone();
                            }
                        }
                    }

                    existing_rows.insert(aligned_row);
                }

                self.data = existing_rows.into_iter().collect();
            }

            op if op.starts_with("UNION_TYPE:LEFT_JOIN_AT{{") => {
                // Extract the join column name from within double curly braces
                let join_column_start = "UNION_TYPE:LEFT_JOIN_AT{{".len();
                let join_column_end = op.find("}}").unwrap_or(op.len());
                let join_column = &op[join_column_start..join_column_end];

                if !self.headers.contains(&join_column.to_string())
                    || !temp_builder.headers.contains(&join_column.to_string())
                {
                    println!(
                        "Join column {{{}}} not found in one or both datasets.",
                        join_column
                    );
                    return self;
                }

                let left_join_index = self.headers.iter().position(|h| h == join_column).unwrap();
                let right_join_index = temp_builder
                    .headers
                    .iter()
                    .position(|h| h == join_column)
                    .unwrap();

                let mut joined_data = Vec::new();

                for left_row in &self.data {
                    let left_join_value = &left_row[left_join_index];

                    let mut combined_row = left_row.clone();

                    // Check for a matching row in the right dataset
                    if let Some(right_row) = temp_builder
                        .data
                        .iter()
                        .find(|right_row| &right_row[right_join_index] == left_join_value)
                    {
                        // Combine data from the right row, avoiding duplicate join column values
                        for (i, value) in right_row.iter().enumerate() {
                            if i != right_join_index {
                                combined_row.push(value.clone());
                            }
                        }
                    } else {
                        // No matching row, append empty strings for right dataset columns
                        combined_row.append(&mut vec![String::new(); unique_headers_temp.len()]);
                    }

                    joined_data.push(combined_row);
                }

                self.data = joined_data;
            }

            op if op.starts_with("UNION_TYPE:RIGHT_JOIN_AT{{") => {
                let join_column_start = "UNION_TYPE:RIGHT_JOIN_AT{{".len();
                let join_column_end = op.find("}}").unwrap_or(op.len());
                let join_column = &op[join_column_start..join_column_end];

                if !self.headers.contains(&join_column.to_string())
                    || !temp_builder.headers.contains(&join_column.to_string())
                {
                    println!(
                        "Join column {{{}}} not found in one or both datasets.",
                        join_column
                    );
                    return self;
                }

                let left_join_index = self.headers.iter().position(|h| h == join_column).unwrap();
                let right_join_index = temp_builder
                    .headers
                    .iter()
                    .position(|h| h == join_column)
                    .unwrap();

                let mut new_headers = temp_builder.headers.clone();
                for header in &self.headers {
                    if !new_headers.contains(header) {
                        new_headers.push(header.clone());
                    }
                }

                let mut joined_data = Vec::new();

                for right_row in &temp_builder.data {
                    let right_join_value = &right_row[right_join_index];
                    let mut combined_row = vec![String::new(); new_headers.len()];

                    // Populate values from the right row
                    for (i, value) in right_row.iter().enumerate() {
                        let target_index = new_headers
                            .iter()
                            .position(|h| h == &temp_builder.headers[i])
                            .unwrap();
                        combined_row[target_index] = value.clone();
                    }

                    // Try to find a matching row in the left dataset
                    if let Some(left_row) = self
                        .data
                        .iter()
                        .find(|left_row| &left_row[left_join_index] == right_join_value)
                    {
                        for (left_i, left_value) in left_row.iter().enumerate() {
                            let left_header = &self.headers[left_i];
                            if left_header != join_column {
                                // Skip the join column
                                let target_index =
                                    new_headers.iter().position(|h| h == left_header).unwrap();
                                combined_row[target_index] = left_value.clone();
                            }
                        }
                    }

                    joined_data.push(combined_row);
                }

                self.headers = new_headers;
                self.data = joined_data;
            }

            _ => {
                println!("Unknown union operation type: {}", union_type);
            }
        }

        self
    }

    /// Performs a flexible intersection operation with the data from another CSV file. This reads data from another CSV file and retains only the rows that are common to both the current data and the new file (aligning common headers). This method retains only the common rows between the current data and the specified CSV file.
    pub fn set_intersection_with(&mut self, file_path: &str) -> &mut Self {
        let temp_builder = CsvBuilder::from_csv(file_path);

        if let Some(error) = temp_builder.error {
            self.error = Some(error);
            return self;
        }

        // Identify common headers
        let common_headers: HashSet<&String> = self.headers.iter().collect();
        let temp_common_headers: HashSet<&String> = temp_builder
            .headers
            .iter()
            .filter(|h| common_headers.contains(h))
            .collect();

        // Map headers to indices for both files
        let self_header_indices: HashMap<_, _> = self
            .headers
            .iter()
            .enumerate()
            .map(|(i, h)| (h, i))
            .collect();
        let temp_header_indices: HashMap<_, _> = temp_builder
            .headers
            .iter()
            .enumerate()
            .map(|(i, h)| (h, i))
            .collect();

        // Align and intersect the data
        let existing_rows: HashSet<Vec<String>> = self.data.drain(..).collect();
        let mut intersected_rows = HashSet::new();

        for row in temp_builder.data {
            let mut aligned_row = vec![String::new(); self.headers.len()];

            // Align data for common headers
            let mut is_common_row = true;
            for header in &temp_common_headers {
                if let (Some(&self_index), Some(&temp_index)) = (
                    self_header_indices.get(header),
                    temp_header_indices.get(header),
                ) {
                    if let Some(value) = row.get(temp_index) {
                        aligned_row[self_index] = value.clone();
                    } else {
                        is_common_row = false;
                        break;
                    }
                }
            }

            if is_common_row && existing_rows.contains(&aligned_row) {
                intersected_rows.insert(aligned_row);
            }
        }

        self.data = intersected_rows.into_iter().collect();

        self
    }

    /// Performs a difference operation with the data from another CSV file. Retains only the rows that are in the current data but not in the new file.
    pub fn set_difference_with(&mut self, file_path: &str) -> &mut Self {
        let temp_builder = CsvBuilder::from_csv(file_path);
        if let Some(error) = temp_builder.error {
            self.error = Some(error);
            return self;
        }

        let other_data: HashSet<Vec<String>> = temp_builder.data.into_iter().collect();
        self.data.retain(|row| !other_data.contains(row));
        self
    }

    /// Performs a symmetric difference operation with the data from another CSV file. Retains only the rows that are in either the current data or the new file, but not in both.
    pub fn set_symmetric_difference_with(&mut self, file_path: &str) -> &mut Self {
        let temp_builder = CsvBuilder::from_csv(file_path);
        if let Some(error) = temp_builder.error {
            self.error = Some(error);
            return self;
        }

        let other_data: HashSet<Vec<String>> = temp_builder.data.into_iter().collect();
        let self_data: HashSet<Vec<String>> = self.data.drain(..).collect();

        let symmetric_difference: HashSet<_> = self_data
            .symmetric_difference(&other_data)
            .cloned()
            .collect();

        self.data = symmetric_difference.into_iter().collect();
        self
    }

    /// Checks if the universe file is universal with respect to both the columns and rows of the instantiated CSV file. Returns the complement if it is universal, otherwise indicates the reason for being non-universal.
    pub fn set_complement_with(
        &mut self,
        universe_file_path: &str,
        exclude_columns: Vec<&str>,
    ) -> &mut Self {
        let universe_builder = CsvBuilder::from_csv(universe_file_path);
        if universe_builder.error.is_some() {
            println!("Error reading the universe file.");
            return self;
        }

        let include_all = exclude_columns.contains(&"INCLUDE_ALL");

        let universe_headers: HashSet<&String> = universe_builder.headers.iter().collect();
        let self_headers: HashSet<&String> = if include_all {
            self.headers.iter().collect()
        } else {
            self.headers
                .iter()
                .filter(|h| !exclude_columns.contains(&h.as_str()))
                .collect()
        };

        // Check for any extra columns in self that are not in universe
        let extra_columns: Vec<_> = self_headers.difference(&universe_headers).collect();
        if !extra_columns.is_empty() {
            println!(
                "Self object has extra columns not present in the universe: {:?}",
                extra_columns
            );
            return self;
        }

        // Filter out excluded columns from universe data
        let universe_data: HashSet<Vec<String>> = universe_builder
            .data
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .filter_map(|(i, value)| {
                        if universe_builder
                            .headers
                            .get(i)
                            .map_or(false, |h| !exclude_columns.contains(&h.as_str()))
                        {
                            Some(value)
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .collect();

        // Filter out excluded columns from self data
        let self_data: HashSet<Vec<String>> = self
            .data
            .iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .filter_map(|(i, value)| {
                        if self
                            .headers
                            .get(i)
                            .map_or(false, |h| !exclude_columns.contains(&h.as_str()))
                        {
                            Some(value.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .collect();

        // Determine rows in the universe but not in self
        let complement: HashSet<_> = universe_data.difference(&self_data).cloned().collect();
        self.data = complement.into_iter().collect();

        self
    }

    pub fn append_derived_boolean_column(
        &mut self,
        new_column_name: &str,
        expressions: Vec<(&str, Exp)>,
        result_expression: &str,
    ) -> &mut Self {
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

    pub fn append_derived_category_column(
        &mut self,
        new_column_name: &str,
        categories: Vec<(&str, Vec<(&str, Exp)>, &str)>,
    ) -> &mut Self {
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

            /*
            // Collect the indices and inputs of top_matches in a separate vector
            let match_indices_and_inputs: Vec<(usize, &str)> = top_matches
                .iter()
                .enumerate()
                .map(|(index, match_)| (index, match_.2))
                .collect();
            */
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
                /*
                // Collect the indices and inputs of top_matches in a separate vector
                let match_indices_and_inputs: Vec<(usize, &str)> = top_matches
                    .iter()
                    .enumerate()
                    .map(|(index, match_)| (index, match_.2))
                    .collect();
                */
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

    /// Pivots a CSV
    pub fn pivot_as<'a>(&'a mut self, path: &str, piv: Piv) -> &mut Self {
        let mut pivot_data: HashMap<String, HashMap<String, Vec<f64>>> = HashMap::new();

        // Finding positions of the necessary columns
        let index_col_pos = match self.headers.iter().position(|x| *x == piv.index_at) {
            Some(pos) => pos,
            None => {
                eprintln!("Error: Index column not found");
                return self;
            }
        };

        let value_col_pos = match self.headers.iter().position(|x| *x == piv.values_from) {
            Some(pos) => pos,
            None => {
                eprintln!("Error: Value column not found");
                return self;
            }
        };

        // Collecting positions and types for the segmentation columns
        let seg_cols_info: Vec<_> = piv
            .seggregate_by
            .iter()
            .filter_map(|&(col, seg_type)| {
                self.headers
                    .iter()
                    .position(|x| x == col)
                    .map(|pos| (pos, seg_type))
            })
            .collect();

        if seg_cols_info.len() != piv.seggregate_by.len() {
            eprintln!("Error: One or more segmentation columns not found");
            return self;
        }

        // Initialize pivot_data with zeros for all indices and segments
        for index in self.data.iter().map(|row| &row[index_col_pos]) {
            let index_value = index.trim().to_string();
            let segments_entry = pivot_data.entry(index_value).or_insert_with(HashMap::new);

            for &(col, seg_type) in &piv.seggregate_by {
                match seg_type {
                    "AS_BOOLEAN" => {
                        segments_entry
                            .entry(col.to_string())
                            .or_insert_with(Vec::new);
                    }
                    "AS_CATEGORY" => {
                        // For each row, insert unique categories into pivot_data
                        for row in &self.data {
                            let category = row[self.headers.iter().position(|x| x == col).unwrap()]
                                .trim()
                                .to_string();
                            segments_entry.entry(category).or_insert_with(Vec::new);
                        }
                    }
                    _ => {
                        eprintln!("Error: Unrecognized segregation type '{}'", seg_type);
                    }
                }
            }
        }

        for row in &self.data {
            let row: Vec<String> = row.iter().map(|cell| cell.trim().to_string()).collect();
            let index_value = &row[index_col_pos];
            let value: f64 = row[value_col_pos].parse().unwrap_or(0.0);

            for &(col, seg_type) in &piv.seggregate_by {
                let col_pos = self.headers.iter().position(|x| x == col).unwrap();

                match seg_type {
                    "AS_BOOLEAN" => {
                        if row[col_pos] == "1" {
                            let seg_key = col.to_string();
                            pivot_data
                                .get_mut(index_value)
                                .unwrap()
                                .get_mut(&seg_key)
                                .unwrap()
                                .push(value);
                        }
                    }
                    "AS_CATEGORY" => {
                        let col_pos = self.headers.iter().position(|x| x == col).unwrap();
                        let category_value = row[col_pos].clone();
                        //dbg!(&col, &col_pos, &category_value);
                        pivot_data
                            .entry(index_value.clone())
                            .or_default()
                            .entry(category_value)
                            .or_default()
                            .push(value);
                    }
                    _ => {
                        println!("Error: Unrecognized segregation type for column '{}'", col);
                    }
                }
            }
        }

        let mut sorted_keys: Vec<_> = pivot_data.keys().collect();
        sorted_keys.sort();

        // Perform operations and write to CSV
        if let Err(e) = (|| -> Result<(), Box<dyn Error>> {
            let mut writer = csv::Writer::from_path(path)?;
            // Write the headers for horizontal format
            let mut headers = vec!["Index"];

            // Add AS_BOOLEAN columns to the headers
            for (col, seg_type) in &piv.seggregate_by {
                if *seg_type == "AS_BOOLEAN" {
                    headers.push(col);
                }
            }

            // Collect all unique AS_CATEGORY values
            let mut all_categories = HashSet::new();
            for (_col, seg_type) in &piv.seggregate_by {
                if *seg_type == "AS_CATEGORY" {
                    for segments in pivot_data.values() {
                        for category in segments.keys() {
                            all_categories.insert(category.clone());
                        }
                    }
                }
            }

            dbg!(&all_categories);

            // Sort the category values if needed and add them to the headers
            let mut sorted_categories: Vec<_> = all_categories.into_iter().collect();
            sorted_categories.sort();

            // Since `headers` is a `Vec<&str>`, we need to map `sorted_categories` to `&str`
            let category_str_slices: Vec<&str> =
                sorted_categories.iter().map(|s| s.as_str()).collect();
            headers.extend(category_str_slices);

            headers.push("Value");
            writer.write_record(&headers)?;

            //let mut total_pushed: bool = false;
            // Write data in horizontal format
            for index in sorted_keys {
                let mut row = vec![index.clone()];
                let segments = pivot_data.get(index).unwrap();
                let mut total: f64 = 0.0;
                let mut total_assessed: bool = false;

                // Add values for AS_BOOLEAN columns
                for (col, seg_type) in &piv.seggregate_by {
                    if *seg_type == "AS_BOOLEAN" {
                        if let Some(segment_values) = segments.get(*col) {
                            let segment_total: f64 = match piv.operation {
                                "COUNT" => segment_values.len() as f64,
                                "SUM" => segment_values.iter().sum(),
                                "MEAN" => {
                                    let sum: f64 = segment_values.iter().sum();
                                    let count = segment_values.len();
                                    if count > 0 {
                                        sum / count as f64
                                    } else {
                                        0.0
                                    }
                                }
                                "MEDIAN" => {
                                    let mut vals = segment_values.to_vec();
                                    vals.sort_unstable_by(|a, b| {
                                        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                    });
                                    if vals.len() % 2 == 1 {
                                        vals[vals.len() / 2]
                                    } else if !vals.is_empty() {
                                        let mid = vals.len() / 2;
                                        (vals[mid - 1] + vals[mid]) / 2.0
                                    } else {
                                        0.0
                                    }
                                }
                                _ => 0.0, // Handle the default case or error
                            };
                            total += segment_total;
                            row.push(format!("{:.2}", segment_total));
                            total_assessed = true;
                        } else {
                            row.push("0.00".to_string()); // Default value if the segment doesn't exist
                        }
                    }
                }

                // Add values for AS_CATEGORY columns
                for category in &sorted_categories {
                    if let Some(segment_values) = segments.get(category) {
                        let segment_total: f64 = match piv.operation {
                            "COUNT" => segment_values.len() as f64,
                            "SUM" => segment_values.iter().sum(),
                            "MEAN" => {
                                let sum: f64 = segment_values.iter().sum();
                                let count = segment_values.len();
                                if count > 0 {
                                    sum / count as f64
                                } else {
                                    0.0
                                }
                            }
                            "MEDIAN" => {
                                let mut vals = segment_values.to_vec();
                                vals.sort_unstable_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                                if vals.len() % 2 == 1 {
                                    vals[vals.len() / 2]
                                } else if !vals.is_empty() {
                                    let mid = vals.len() / 2;
                                    (vals[mid - 1] + vals[mid]) / 2.0
                                } else {
                                    0.0
                                }
                            }
                            _ => 0.0, // Handle the default case or error
                        };
                        if total_assessed == false {
                            total += segment_total;
                        }
                        row.push(format!("{:.2}", segment_total));
                    } else {
                        row.push("0.00".to_string()); // Default value if the segment doesn't exist
                    }
                }

                // Add the total value at the end of the row
                //if total_pushed == false{
                row.push(format!("{:.2}", total));
                //total_pushed = true;
                //}

                // Ensure the row has the same number of fields as the headers
                if row.len() != headers.len() {
                    eprintln!(
                        "Error: Row length {} does not match headers length {}",
                        row.len(),
                        headers.len()
                    );
                    continue; // Skip this row or handle the error as needed
                }

                writer.write_record(&row)?;
            }

            writer.flush()?;
            Ok(())
        })() {
            eprintln!("Error writing to CSV: {}", e);
        }

        self
    }

    #[allow(unreachable_code)]
    pub fn die(&mut self) -> &mut Self {
        println!("Giving up the ghost!");
        std::process::exit(0);
        self
    }

    // Method to check if the builder has any data (headers or rows)
    pub fn has_data(&self) -> bool {
        !self.headers.is_empty() || !self.data.is_empty()
    }

    // Method to check if the builder has headers
    pub fn has_headers(&self) -> bool {
        !self.headers.is_empty()
    }

    // Method to get the headers
    pub fn get_headers(&self) -> Option<&[String]> {
        if self.has_headers() {
            Some(&self.headers)
        } else {
            None
        }
    }

    // Method to get a reference to the CSV data
    pub fn get_data(&self) -> &Vec<Vec<String>> {
        &self.data
    }
}

/// Represents a caching mechanism for CSV results, holding a data generator, cache path, and cache duration.
pub struct CsvResultCacher {
    data_generator:
        Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>>>> + Send + Sync>,
    cache_path: String, // Still using String here to store the path
    cache_duration: Duration,
}

impl CsvResultCacher {
    /// Constructs a new `CsvResultCacher` with a specified data generator, cache path, and cache duration in minutes.
    pub fn new<F>(data_generator: F, cache_path: String, cache_duration_minutes: u64) -> Self
    where
        F: Fn() -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>>>>
            + Send
            + Sync
            + 'static,
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
        F: Fn() -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>>>>
            + Send
            + Sync
            + 'static,
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
