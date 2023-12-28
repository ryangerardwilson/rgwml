// csv_utils.rs
use chrono::{DateTime, NaiveDateTime};
use csv::Writer;
use futures::executor::block_on;
use futures::future::join_all;
use futures::Future;
use fuzzywuzzy::fuzz;
use regex::Regex;
use serde_json::Value;
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
    pub fn get_docs() -> String {
        let docs = r#"

++++++++++++++++++++++++++++++++
+> CsvConverter Documentation <+
++++++++++++++++++++++++++++++++

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

"#;
        // docs.to_string();

        println!("{}", docs.to_string());

        docs.to_string()
    }

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
                "DOES_NOT_CONTAIN" => !cell_value.contains(*self),
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
    /// A function to print documentation for easy reference
    pub fn get_docs() -> String {
        let docs = r#"

++++++++++++++++++++++++++++++
+> CsvBuilder Documentation <+
++++++++++++++++++++++++++++++

1. Instantiation
----------------

Example 1: Creating a new object

    use rgwml::csv_utils::CsvBuilder;

    let builder = CsvBuilder::new()
        .set_header(&["Column1", "Column2", "Column3"])
        .add_rows(&[&["Row1-1", "Row1-2", "Row1-3"], &["Row2-1", "Row2-2", "Row2-3"]])
        .save_as("/path/to/your/file.csv");

Example 2: Load from an existing file

    use rgwml::csv_utils::CsvBuilder;

    let builder = CsvBuilder::from_csv("/path/to/existing/file.csv");

2. Manipulating a CsvBuilder Object for Analysis or Saving
----------------------------------------------------------

    use rgwml::csv_utils::CsvBuilder;

    let _ = CsvBuilder::from_csv("/path/to/your/file.csv")
        .rename_columns(vec![("OLD_COLUMN", "NEW_COLUMN")])
        .drop_columns(vec!["UNUSED_COLUMN"])
        .cascade_sort(vec![("COLUMN", "ASC")])
        .where_("address","FUZZ_MIN_SCORE_70",vec!["new delhi","jerusalem"], "COMPARE_AS_TEXT") // Adjust score value to any two digit number like FUZZ_MIN_SCORE_23, FUZZ_MIN_SCORE_67, etc.
        .print_row_count()
        .save_as("/path/to/modified/file.csv");

3. Chainable Options
--------------------

    CsvBuilder::from_csv("/path/to/your/file1.csv")
    // A. Setting and adding headers
    .set_header(vec!["Header1", "Header2", "Header3"])
    .add_column_header("NewColumn1")
    .add_column_headers(vec!["NewColumn2", "NewColumn3"])
    
    // B. Ordering columns
    .order_columns(vec!["Column1", "...", "Column5", "Column2"])
    .order_columns(vec!["...", "Column5", "Column2"])
    .order_columns(vec!["Column1", "Column5", "..."])
    
    // C. Modifying columns
    .drop_columns(vec!["Column1", "Column3"])
    .rename_columns(vec![("Column1", "NewColumn1"), ("Column3", "NewColumn3")])
    
    // D. Adding and modifying rows
    .add_row(vec!["Row1-1", "Row1-2", "Row1-3"])
    .add_rows(vec![vec!["Row1-1", "Row1-2", "Row1-3"], vec!["Row2-1", "Row2-2", "Row2-3"]])
    .remove_duplicates()
    
    // E. Cleaning/ Replacing Cell values
    .trim_all() // Trims white spaces at the beginning and end of all cells in all columns.
    .replace_all(vec!["Column1", "Column2"], vec![("null", ""), ("NA", "-")]) // In specified columns
    .replace_all(vec!["*"], vec![("null", ""), ("NA", "-")]) // In all columns
    
    // F. Limiting and sorting
    .limit(10)
    .cascade_sort(vec![("Column1", "DESC"), ("Column3", "ASC")])
    
    // G. Applying conditional operations
    .where_("column1", "==", "42", "COMPARE_AS_NUMBERS")
    .where_("column1", "==", "hello", "COMPARE_AS_TEXT")
    .where_("column1", "CONTAINS", "apples", "COMPARE_AS_TEXT")
    .where_("column1", "DOES_NOT_CONTAIN", "apples", "COMPARE_AS_TEXT")
    .where_("column1", "STARTS_WITH", "discounted", "COMPARE_AS_TEXT")
    .where_("stated_locality_address","FUZZ_MIN_SCORE_90",vec!["Shastri park","kamal nagar"], "COMPARE_AS_TEXT")
    .where_("column1", ">", "23-01-01", "COMPARE_AS_TIMESTAMPS")
    .where_set("column1", "==", "hello", "COMPARE_AS_TEXT", "Column9", "greeting")

    // H. Analytical Prints for data inspection
    .print_columns()
    .print_row_count()
    .print_first_row()
    .print_last_row()
    .print_rows_range(2,5)
    .print_rows()
    .print_unique("column_name")
    .print_freq(vec!["Column1", "Column2"])

    // I. Grouping Data
    .split_as("ColumnNameToGroupBy", "/output/folder/for/grouped/csv/files/") // Groups data by a specified column and saves each group into a separate CSV file in a given folder

    // J. Basic Set Theory Operations (for the Universe U = {1,2,3,4,5,6,7}, A = {1,2,3} and B = {3,4,5})
    .set_union_with("/path/to/set_b/file.csv", "UNION_TYPE:ALL") // {1,2,3,3,4,5} 
    .set_union_with("/path/to/set_b/file.csv", "UNION_TYPE:ALL_WITHOUT_DUPLICATES") // {1,2,3,4,5}
    .set_intersection_with("/path/to/set_b/file.csv") // {3}
    .set_difference_with("/path/to/set_b/file.csv") // {1,2} i.e. in A but not in B
    .set_symmetric_difference_with("/path/to/set_b/file.csv") // {1,2,4,5} i.e. in either, but not in intersection
    
    // .set_complement_with determines the compliment qua the universe i.e. {4,5,6,7}. Pass an exclusion vector to exclude specific columns of the universe from consideration, or use vec!["INCLUDE_ALL"] to include all columns of the universe.
    .set_complement_with("/path/to/universe_set_u/file.csv", vec!["INCLUDE_ALL"]) 
    .set_complement_with("/path/to/universe_set_u/file.csv", vec!["Column4", "Column5"]) 

    // K. Advanced Set Theory Operations
    .set_union_with("/path/to/table_b.csv", "UNION_TYPE:LEFT_JOIN_AT{{Column1}}") // Left join using "Column1" as the join column.
    .set_union_with("/path/to/table_b.csv", "UNION_TYPE:RIGHT_JOIN_AT{{Column1}}") // Right join using "ID" as the join column.

    // L. Save
    .save_as("/path/to/your/file2.csv")

4. Extract Data
---------------

These methods return a CsvBuilder object, and hence, can not be subsequently chained.

    CsvBuilder::from_csv("/path/to/your/file1.csv")

    .get_unique("column_name"); // Returns a Vec<String>
    .get("column_name"); // Returns cell content as a String, if the csv has been filtered to single row.
    .get_freq(vec!["Column1", Column2]) // Returns a HashMap where keys are column names and values are vectors of sorted (value, frequency) pairs.

"#;
        // docs.to_string();

        println!("{}", docs.to_string());

        docs.to_string()
    }

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
        if self.error.is_none() {
            let header_row = header.into_iter().map(|s| s.to_string()).collect();
            self.data.insert(0, header_row);
        }
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

    /// Adds column header
    pub fn add_column_header(&mut self, column_name: &str) -> &mut Self {
        if self.error.is_none() {
            self.headers.push(column_name.to_string());
        }
        self
    }

    /// Adds multiple column headers
    pub fn add_column_headers(&mut self, column_names: Vec<&str>) -> &mut Self {
        if self.error.is_none() {
            for &column_name in column_names.iter() {
                self.headers.push(column_name.to_string());
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

    /// Sets a column's value based on a condition applied to a different column.
    pub fn where_set<T: CompareValue>(
        &mut self,
        filter_column_name: &str,
        operation: &str,
        value: T, // Value to check against in the filter column.
        compare_as: &str,
        set_column_name: &str,
        set_value: &str, // Value to set in the target column.
    ) -> &mut Self {
        if let Some(filter_column_index) = self.headers.iter().position(|h| h == filter_column_name)
        {
            if let Some(set_column_index) = self.headers.iter().position(|h| h == set_column_name) {
                self.data.iter_mut().for_each(|row| {
                    if let Some(cell_value) = row.get(filter_column_index) {
                        if value.apply(cell_value, operation, compare_as) {
                            if let Some(target) = row.get_mut(set_column_index) {
                                *target = set_value.to_string();
                            }
                        }
                    }
                });
            } else {
                println!("Set column '{}' not found in headers.", set_column_name);
            }
        } else {
            println!(
                "Filter column '{}' not found in headers.",
                filter_column_name
            );
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
}

/// Represents a caching mechanism for CSV results, holding a data generator, cache path, and cache duration.
pub struct CsvResultCacher {
    data_generator:
        Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>>>> + Send + Sync>,
    cache_path: String, // Still using String here to store the path
    cache_duration: Duration,
}

impl CsvResultCacher {
    pub fn get_docs() -> String {
        let docs = r#"

+++++++++++++++++++++++++++++++++++
+> CsvResultCacher Documentation <+
+++++++++++++++++++++++++++++++++++

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
            .save_as("/path/to/daily_sales_report.csv");

        Ok(())
    }

    #[tokio::main]
    async fn main() {
        let cache_path = "/path/to/daily_sales_report.csv";
        let cache_duration_minutes = 1440; // Cache duration set to 1 day

        let result = CsvResultCacher::fetch_async(
            || Box::pin(generate_daily_sales_report()),
            cache_path,
            cache_duration_minutes,
        ).await;

        match result {
            Ok(_) => println!("Sales report is ready."),
            Err(e) => eprintln!("Failed to generate sales report: {}", e),
        }
    }

"#;
        // docs.to_string();

        println!("{}", docs.to_string());

        docs.to_string()
    }

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
