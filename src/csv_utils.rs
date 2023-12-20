// csv_utils.rs
use crate::df_utils::DataFrame;
use csv::{Reader, StringRecord, Writer};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::{self, Write};

/// A flexible builder for creating and writing to CSV files.
///
/// This struct allows for a fluent interface to build and write to a CSV file,
/// supporting method chaining. It uses a generic writer to handle different types
/// of outputs, primarily working with file-based writing.
pub struct CsvBuilder {
    headers: Vec<String>,
    data: Vec<Vec<String>>,
    error: Option<Box<dyn Error>>,
}

impl CsvBuilder {
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
    ///
    /// # Example
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::df_utils::DataFrame; // Replace with actual DataFrame type
    ///
    /// let data_frame = // Your DataFrame initialization here
    /// CsvBuilder::from_dataframe(data_frame)
    ///     .add_row(&["Row1-1", "Row1-2", "Row1-3"])
    ///     .add_rows(&[&["Row2-1", "Row2-2", "Row2-3"], &["Row3-1", "Row3-2", "Row3-3"]])
    ///     .save_as("/path/to/your/file.csv");
    /// ```
    ///
    /// Creates a `CsvBuilder` from a DataFrame, adds rows, and saves the data as a new CSV file.

    pub fn save_as(&self, new_file_path: &str) -> Result<(), Box<dyn Error>> {
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
    ///
    /// # Example
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new()
    ///     .set_header(&["Column1", "Column2", "Column3"])
    ///     .add_row(&["Row1-1", "Row1-2", "Row1-3"])
    ///     .save_as("/path/to/your/file.csv");
    /// ```
    ///
    /// Creates a `CsvBuilder` with the specified header, adds a data row, and then
    /// saves the data as a new CSV file.    
    pub fn set_header(mut self, header: &[&str]) -> Self {
        if self.error.is_none() {
            let header_row = header.iter().map(|s| s.to_string()).collect();
            self.data.insert(0, header_row);
        }
        self
    }

    /// Adds a data row to the CSV.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new()
    ///     .set_header(&["Column1", "Column2", "Column3"])
    ///     .add_row(&["Row1-1", "Row1-2", "Row1-3"])
    ///     .save("/path/to/your/file.csv");
    /// ```
    ///
    /// Creates a `CsvBuilder`, adds a data row, and then saves the data as a new CSV file.

    pub fn add_row(mut self, row: &[&str]) -> Self {
        if self.error.is_none() {
            let row_vec = row.iter().map(|s| s.to_string()).collect();
            self.data.push(row_vec);
        }
        self
    }

    /// Adds multiple data rows to the CSV.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new()
    ///     .set_header(&["Column1", "Column2", "Column3"])
    ///     .add_rows(&[&["Row1-1", "Row1-2", "Row1-3"], &["Row2-1", "Row2-2", "Row2-3"]])
    ///     .save("/path/to/your/file.csv");
    /// ```
    ///
    /// Creates a `CsvBuilder`, adds multiple data rows, and then saves the data as a new CSV file.

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

    /// ## Examples
    ///
    /// ### Moving Columns to the Start of the Order
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new()
    ///     .set_header(&["Column3", "Column1", "Column2"])
    ///     .add_rows(&[&["Row2-3", "Row2-1", "Row2-2"], &["Row1-3", "Row1-1", "Row1-2"]])
    ///     .column_order(vec!["Column1", "Column2", "...", "Column3"])
    ///     .save("/path/to/your/file.csv");
    /// ```
    ///
    /// In this example, we have a CSV with columns `"Column3"`, `"Column1"`, and `"Column2"`.
    /// By specifying `"Column1"`, `"Column2"`, `...`, and then `"Column3"` in the `column_order` method,
    /// we indicate that `"Column1"` and `"Column2"` should be moved to the start of the column order,
    /// and `"Column3"` should follow.

    /// ### Moving Columns to the End of the Order
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new()
    ///     .set_header(&["Column1", "Column2", "Column3"])
    ///     .add_rows(&[&["Row1-1", "Row1-2", "Row1-3"], &["Row2-1", "Row2-2", "Row2-3"]])
    ///     .column_order(vec!["Column3", "Column1", "...", "Column2"])
    ///     .save("/path/to/your/file.csv");
    /// ```
    ///
    /// In this example, we have a CSV with columns `"Column1"`, `"Column2"`, and `"Column3"`.
    /// By specifying `"Column3"`, `"Column1"`, `...`, and then `"Column2"` in the `column_order` method,
    /// we indicate that `"Column3"` and `"Column1"` should be moved to the end of the column order,
    /// and `"Column2"` should follow.

    /// ### Placing Columns in Between
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new()
    ///     .set_header(&["Column1", "Column4", "Column5", "Column2"])
    ///     .add_rows(&[&["Row1-1", "Row1-4", "Row1-5", "Row1-2"], &["Row2-1", "Row2-4", "Row2-5", "Row2-2"]])
    ///     .column_order(vec!["Column1", "...", "Column5", "Column2"])
    ///     .save("/path/to/your/file.csv");
    /// ```
    ///
    /// In this example, we have a CSV with columns `"Column1"`, `"Column4"`, `"Column5"`, and `"Column2"`.
    /// By specifying `"Column1"`, `...`, `"Column5"`, and `"Column2"` in the `column_order` method,
    /// we indicate that `"Column1"` should be placed at the start, `"Column5"` should be placed between
    /// `"Column1"` and `"Column2"`, and `"Column4"` retains its original position.

    pub fn column_order(mut self, order: Vec<&str>) -> Self {
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

    /// Sorts the CSV data based on specified column orders.
    ///
    /// # Parameters
    ///
    /// - `orders`: A vector of tuples specifying column names and sorting orders (e.g., [("Column1", "ASC"), ("Column2", "DESC")]).
    ///
    /// # Example
    ///
    /// ```rust
    /// use rgwml::csv_utils::CsvBuilder;
    ///
    /// let builder = CsvBuilder::new()
    ///     .set_header(&["Column1", "Column2", "Column3"])
    ///     .add_rows(&[&["Row2-1", "Row2-2", "Row2-3"], &["Row1-1", "Row1-2", "Row1-3"]])
    ///     .column_order(vec!["Column1", "...", "Column5", "Column2"])
    ///     .cascade_sort(vec![("Column1", "DESC"), ("Column3", "ASC")]) // WARNING! MUST BE CHAINED
    ///     AFTER column_order
    ///     .save("/path/to/your/file.csv");
    /// ```
    ///
    /// Creates a `CsvBuilder`, adds data rows, performs a cascading sort based on column orders (must be applied after `set_header` and `add_rows`), and then saves the data as a new CSV file.

    pub fn cascade_sort(mut self, orders: Vec<(&str, &str)>) -> Self {
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
}
