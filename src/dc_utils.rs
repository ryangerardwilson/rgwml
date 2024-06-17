// dc_utils.rs
use crate::csv_utils::CsvBuilder;
use chrono::{DateTime, TimeZone, Utc};
use std::time::UNIX_EPOCH;

use crate::python_utils::DC_CONNECT_SCRIPT;
use calamine::{open_workbook, Reader, Xls, Xlsx};
use dirs;
use hdf5::File as H5File;
use lazy_static::lazy_static;
use memmap::MmapOptions;
use serde_json::Value;
use std::error::Error;
use std::fs::{create_dir_all, metadata, read_dir, remove_file, File};
use std::io::{BufWriter, Write};
use std::process::Command;
use tokio::runtime::Runtime;
use uuid::Uuid;

const LIBRARY_VERSION: &str = "1.3.19";

lazy_static! {
    static ref INITIALIZE: () = {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            if let Err(e) = prepare_executable().await {
                eprintln!("Initialization error: {}", e);
            }
        });
    };
}

// Ensures the lazy_static block is included in the library initialization
#[allow(dead_code)]
fn ensure_initialized() {
    lazy_static::initialize(&INITIALIZE);
}

// Ensure initialization occurs on import
#[ctor::ctor]
fn init() {
    ensure_initialized();
}

/// Prepares the executable asynchronously and returns the result as a string.
async fn prepare_executable() -> Result<(), Box<dyn std::error::Error>> {
    let version = LIBRARY_VERSION;

    let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
    let rgwml_dir = home_dir.join("RGWML");
    let exec_dir = rgwml_dir.join("executables");

    // Create necessary directories
    if !rgwml_dir.exists() {
        create_dir_all(&rgwml_dir)?;
    }
    if !exec_dir.exists() {
        create_dir_all(&exec_dir)?;
    }

    // Create the versioned Python file name
    let versioned_python_file_name = format!("dc_connect_v{}.py", version.replace('.', "_"));
    let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

    // Remove old version files that do not match the current version
    for entry in read_dir(&exec_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        if file_name_str.starts_with("dc_connect_v") && file_name_str != versioned_python_file_name
        {
            //println!("Removing old file: {}", file_name_str); // Debugging information
            remove_file(entry.path())?;
        }
    }

    // Check if the versioned Python file already exists
    if !versioned_python_file_path.exists() {
        // Write the Python script to the versioned file
        let mut out = BufWriter::new(File::create(&versioned_python_file_path)?);
        out.write_all(DC_CONNECT_SCRIPT.as_bytes())?;
        out.flush()?;
    }

    Ok(())
}

/// Represents a pivot transformation on a dataset. This struct allows you to specify how to transform a dataset by pivoting one of its columns.
#[derive(Debug)]
pub struct DcConnectConfig {
    pub path: String,
    pub dc_type: String,               // Options: H5
    pub h5_dataset_identifier: String, // Relevant for only H5; Name or index of dataset
    pub h5_identifier_type: String,    // Relevant only for H5; Options: DATASET_NAME/ DATASET_ID
}

/// Represents a DataContainer
pub struct DataContainer;

/// Implements a DataContainer
impl DataContainer {
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dc_utils::{DcConnectConfig, DataContainer};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// // Append the file name to the directory path
    /// let h5_path = current_dir.join("test_file_samples/h5_test_files/sample.h5");
    ///
    /// // Convert the path to a string
    /// let h5_path_str = h5_path.to_str().unwrap();
    ///
    ///     let dc_connect_config = DcConnectConfig {
    ///         path: h5_path_str.to_string(),
    ///         dc_type: "H5".to_string(),
    ///         h5_dataset_identifier: "0".to_string(),
    ///         h5_identifier_type: "DATASET_ID".to_string(),
    ///     };
    ///
    ///     let result = DataContainer::get_dc_data(dc_connect_config).await;
    ///
    ///     match result {
    ///         Ok(res) => {
    ///             dbg!(&res);
    ///             // Add meaningful assertion here based on expected result
    ///             // Example:
    ///             assert!(!res.0.is_empty(), "Expected non-empty feature names");
    ///         }
    ///         Err(e) => {
    ///             dbg!(e);
    ///             // Add meaningful assertion here based on expected error
    ///             // Example:
    ///             assert!(false, "Expected training to succeed but it failed");
    ///         }
    ///     }
    /// });
    /// ```
    pub async fn get_dc_data(
        dc_connect_config: DcConnectConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name = format!("dc_connect_v{}.py", version.replace('.', "_"));
        let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

        // Return the full command to execute the Python script
        let temp_path = format!("python3 {}", versioned_python_file_path.to_string_lossy());

        let mut args = Vec::new();

        // Helper macro to add optional arguments if they are not empty
        macro_rules! add_arg {
            ($flag:expr, $value:expr) => {
                if !$value.is_empty() {
                    args.push(format!("{} \"{}\"", $flag, $value));
                }
            };
        }

        let uid = format!(
            "{}-{}",
            Uuid::new_v4().to_string(),
            Utc::now().timestamp_millis()
        );
        add_arg!("--uid", &uid);

        add_arg!("--path", &dc_connect_config.path);
        add_arg!("--dc_type", &dc_connect_config.dc_type);
        add_arg!(
            "--h5_dataset_identifier",
            &dc_connect_config.h5_dataset_identifier
        );
        add_arg!(
            "--h5_identifier_type",
            &dc_connect_config.h5_identifier_type
        );

        let command_str = format!("{} {}", temp_path, args.join(" "));
        let output = Command::new("sh").arg("-c").arg(&command_str).output();

        match output {
            Ok(output) => {
                //dbg!(&output);
                if !output.status.success() {
                    let stderr = std::str::from_utf8(&output.stderr)?;
                    //eprintln!("Script error: {}", stderr);
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        stderr,
                    )));
                }

                //let output_str = std::str::from_utf8(&output.stdout)?;
                //let file = File::open("output.json")?;

                let filename = format!("rgwml_{}.json", &uid);
                let file = File::open(&filename)?;

                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

                //println!("Output from the command: {}", output_str);

                let json: Value = serde_json::from_str(output_str)
                    .map_err(|e| format!("Failed to parse JSON: {}", e))?;

                //println!("Parsed JSON response: {:?}", json);

                let headers = json["headers"]
                    .as_array()
                    .ok_or("Headers missing in JSON")?
                    .iter()
                    .map(|v| v.as_str().unwrap_or("").to_string())
                    .collect();

                let rows = json["rows"]
                    .as_array()
                    .ok_or("Rows missing in JSON")?
                    .iter()
                    .map(|row| {
                        row.as_array()
                            .unwrap_or(&vec![])
                            .iter()
                            .map(|cell| cell.as_str().unwrap_or("").to_string())
                            .collect()
                    })
                    .collect();

                remove_file(filename)?;
                Ok((headers, rows))
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// Scans the specified directory for .csv, .h5, .xls, and .xlsx files and returns a `CsvBuilder` containing their metadata.
    ///
    /// # Example
    ///
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dc_utils::DataContainer;
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
    ///
    /// let mut csv_builder = DataContainer::get_all_data_files(csv_path).expect("Failed to load data files");
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
    pub fn get_all_data_files(path: &str) -> Result<CsvBuilder, Box<dyn Error>> {
        // Read the directory
        let dir_entries = read_dir(path)?;

        // Create a new CsvBuilder and set the headers
        let mut csv_builder = CsvBuilder::new();
        csv_builder.set_header(vec!["file_name", "last_modified", "mb_size"]);

        // Iterate through each file in the directory
        for entry in dir_entries {
            let entry = entry?;
            let file_path = entry.path();

            // Process .csv, .h5, .xls, and .xlsx files
            if let Some(ext) = file_path.extension().and_then(|s| s.to_str()) {
                if ["csv", "h5", "xls", "xlsx"].contains(&ext) {
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
                    csv_builder.add_row(vec![
                        &file_name,
                        &formatted_timestamp,
                        &formatted_file_size,
                    ]);
                }
            }
        }

        Ok(csv_builder)
    }

    /// Returns a `Vec<String>` of the dataset names in an HDF5 file at the specified `file_path`.
    ///
    /// # Example
    ///
    /// ```
    /// use rgwml::dc_utils::DataContainer;
    /// use hdf5::{File as H5File};
    /// use hdf5::types::FixedAscii;
    /// use ndarray::Array2;
    /// use std::env;
    /// use std::path::PathBuf;
    ///
    /// fn create_fixed_ascii<const N: usize>(s: &str) -> FixedAscii<N> {
    ///     FixedAscii::from_ascii(s).unwrap()
    /// }
    ///
    /// // Create an HDF5 file for testing
    /// fn create_test_hdf5(file_name: &str) -> PathBuf {
    ///     let mut path = env::current_dir().unwrap();
    ///     path.push("test_file_samples");
    ///     std::fs::create_dir_all(&path).unwrap();
    ///     path.push(file_name);
    ///     let file = H5File::create(&path).unwrap();
    ///     let data1: Array2<FixedAscii<6>> = Array2::from_shape_vec((2, 3), vec![
    ///         create_fixed_ascii("col1"), create_fixed_ascii("col2"), create_fixed_ascii("col3"),
    ///         create_fixed_ascii("1"), create_fixed_ascii("2"), create_fixed_ascii("3"),
    ///     ]).unwrap();
    ///     let dataset1 = file.new_dataset::<FixedAscii<6>>().shape((2, 3)).create("dataset_name1").unwrap();
    ///     dataset1.write(&data1).unwrap();
    ///     
    ///     let data2: Array2<FixedAscii<6>> = Array2::from_shape_vec((2, 3), vec![
    ///         create_fixed_ascii("a"), create_fixed_ascii("b"), create_fixed_ascii("c"),
    ///         create_fixed_ascii("4"), create_fixed_ascii("5"), create_fixed_ascii("6"),
    ///     ]).unwrap();
    ///     let dataset2 = file.new_dataset::<FixedAscii<6>>().shape((2, 3)).create("dataset_name2").unwrap();
    ///     dataset2.write(&data2).unwrap();
    ///     
    ///     let data3: Array2<FixedAscii<6>> = Array2::from_shape_vec((2, 3), vec![
    ///         create_fixed_ascii("x"), create_fixed_ascii("y"), create_fixed_ascii("z"),
    ///         create_fixed_ascii("7"), create_fixed_ascii("8"), create_fixed_ascii("9"),
    ///     ]).unwrap();
    ///     let dataset3 = file.new_dataset::<FixedAscii<6>>().shape((2, 3)).create("dataset_name3").unwrap();
    ///     dataset3.write(&data3).unwrap();
    ///     
    ///     path
    /// }
    ///
    /// let test_file_path = create_test_hdf5("file_example.h5");
    /// let dataset_names = DataContainer::get_h5_dataset_names(test_file_path.to_str().unwrap());
    /// assert!(dataset_names.is_ok());
    /// let names = dataset_names.unwrap();
    /// assert_eq!(names, vec!["dataset_name1", "dataset_name2", "dataset_name3"]);
    /// ```
    ///
    pub fn get_h5_dataset_names(file_path: &str) -> Result<Vec<String>, Box<dyn Error>> {
        // Open the HDF5 file
        let file = H5File::open(file_path)?;

        // Get the names of the datasets in the file
        let dataset_names = file.member_names()?;

        Ok(dataset_names)
    }

    /// Returns a `Vec<String>` of the sheet names in an XLS file at the specified `file_path`.
    ///
    /// # Example
    ///
    /// ```
    /// use rgwml::dc_utils::DataContainer;
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
    /// let sheet_names = DataContainer::get_xls_sheet_names(test_file_path.to_str().unwrap());
    /// assert!(sheet_names.is_ok());
    /// let names = sheet_names.unwrap();
    /// assert_eq!(names, vec!["Sheet1", "Sheet2", "Sheet3"]);
    /// ```
    pub fn get_xls_sheet_names(file_path: &str) -> Result<Vec<String>, Box<dyn Error>> {
        // Open the XLS file
        let workbook: Xls<_> = open_workbook(file_path)?;

        // Get the names of the sheets in the file
        let sheet_names = workbook.sheet_names().to_vec();

        Ok(sheet_names)
    }

    /// Returns a `Vec<String>` of the sheet names in an XLSX file at the specified `file_path`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rgwml::dc_utils::DataContainer;
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
    /// let sheet_names = DataContainer::get_xlsx_sheet_names(test_file_path.to_str().unwrap());
    /// assert!(sheet_names.is_ok());
    /// let names = sheet_names.unwrap();
    /// assert_eq!(names, vec!["Sheet1", "Sheet2", "Sheet3"]);
    /// ```
    pub fn get_xlsx_sheet_names(file_path: &str) -> Result<Vec<String>, Box<dyn Error>> {
        // Open the XLSX file
        let workbook: Xlsx<_> = open_workbook(file_path)?;

        // Get the names of the sheets in the file
        let sheet_names = workbook.sheet_names().to_vec();

        Ok(sheet_names)
    }
}
