// csv_utils.rs
use csv::Writer;
use std::error::Error;
use std::fs::File;
use std::io::{self, Write};

/// A flexible builder for creating and writing to CSV files.
///
/// This struct allows for a fluent interface to build and write to a CSV file,
/// supporting method chaining. It uses a generic writer to handle different types
/// of outputs, primarily working with file-based writing.
///
/// # Examples
///
/// Basic usage:
///
/// ```
/// use rgwml::csv_utils::CsvBuilder;
///
/// let result = CsvBuilder::new("/path/to/your/file.csv")
///     .set_header(&["Column1", "Column2", "Column3"])
///     .add_row(&["Row1-1", "Row1-2", "Row1-3"])
///     .add_rows(&[&["Row2-1", "Row2-2", "Row2-3"], &["Row3-1", "Row3-2", "Row3-3"]]);
/// ```
///
/// This example demonstrates creating a new CSV file, setting its header,
/// adding individual rows, and a collection of rows. The builder pattern allows
/// for these methods to be chained for ease of use.
pub struct CsvBuilder {
    writer: Writer<Box<dyn Write>>,
    error: Option<Box<dyn Error>>,
    file_path: String,
}

impl CsvBuilder {
    /// Creates a new CsvBuilder for the specified file path.
    pub fn new(file_path: &str) -> Self {
        let writer: Box<dyn Write> = match File::create(file_path) {
            Ok(file) => Box::new(file),
            Err(e) => {
                return CsvBuilder { 
                    writer: Writer::from_writer(Box::new(io::sink())), 
                    error: Some(Box::new(e)),
                    file_path: file_path.to_string(),
                };
            }
        };
        CsvBuilder { 
            writer: Writer::from_writer(writer), 
            error: None,
            file_path: file_path.to_string(),
        }
    }
    /// Sets the header of the CSV file.
    pub fn set_header(mut self, header: &[&str]) -> Self {
        if self.error.is_none() {
            if let Err(e) = self.writer.write_record(header) {
                self.error = Some(Box::new(e));
            }
        }
        self
    }

    /// Adds a single row to the CSV file.
    pub fn add_row(mut self, row: &[&str]) -> Self {
        if self.error.is_none() {
            if let Err(e) = self.writer.write_record(row) {
                self.error = Some(Box::new(e));
            }
        }
        self
    }

    /// Adds multiple rows to the CSV file.
    pub fn add_rows(mut self, rows: &[&[&str]]) -> Self {
        if self.error.is_none() {
            for row in rows {
                if let Err(e) = self.writer.write_record(*row) {
                    self.error = Some(Box::new(e));
                    break;
                }
            }
        }
        self
    }

}

impl Drop for CsvBuilder {
    fn drop(&mut self) {
        if let Some(_) = &self.error {
            // If there was an error, don't print the success message
            return;
        }

        if let Err(_) = self.writer.flush() {
            eprintln!("Error occurred while finalizing CSV file.");
            return;
        }

        println!("CSV file has been successfully created at {}", self.file_path);
    }
}
