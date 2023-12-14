use std::error::Error;
use std::fs::File;
use csv::Writer;

/// Creates a CSV file at the specified path with the given header.
///
/// # Arguments
///
/// * `file_path` - A string specifying the path where the CSV file will be created.
/// * `header` - A slice of strings representing the header row of the CSV file.
///
/// # Returns
///
/// This function returns a `Result<(), Box<dyn Error>>`. On success, it returns `Ok(())`.
/// On failure, it returns an error wrapped in a `Box<dyn Error>`.
///
/// # Examples
///
/// ```
/// use rgwml::csv_utils::create_csv;
///
/// let result = create_csv("/path/to/output.csv".to_string(), &["Column1", "Column2", "Column3"]);
/// assert!(result.is_ok());
/// ```
pub fn create_csv(file_path: String, header: &[&str]) -> Result<(), Box<dyn Error>> {
    let file = File::create(&file_path)?;

    let mut wtr = Writer::from_writer(file);

    // Write the header row
    wtr.write_record(header)?;

    wtr.flush()?;
    println!("CSV file has been created at {}", file_path);
    Ok(())
}

