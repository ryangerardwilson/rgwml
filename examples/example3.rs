use std::error::Error;
use std::fs::File;
use std::io::Write;
use csv::Writer;

fn create_csv(file_path: String, header: &[&str]) -> Result<(), Box<dyn Error>> {
    let mut file = File::create(file_path)?;

    let mut wtr = Writer::from_writer(file);

    // Write the header row
    wtr.write_record(header)?;

    // Optionally, you can also add rows of data here
    // For example:
    // wtr.write_record(&["Value1", "Value2", "Value3"])?;

    wtr.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Call the create_csv function with an absolute path
    let absolute_path = "/home/rgw/Desktop/output.csv"; // Replace with your actual path
    create_csv(absolute_path.to_string(), &["Column1", "Column2", "Column3"])
}

