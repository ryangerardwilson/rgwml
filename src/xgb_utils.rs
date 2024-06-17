// xgb_utils.rs

use crate::csv_utils::CsvBuilder;
use crate::python_utils::XGB_CONNECT_SCRIPT;
use chrono::{DateTime, TimeZone, Utc};
use dirs;
use lazy_static::lazy_static;
use memmap::MmapOptions;
use serde_json::Value;
use std::error::Error;
use std::fs::{create_dir_all, metadata, read_dir, remove_file, File};
use std::io::{BufReader, BufWriter, Write};
use std::process::Command;
use std::time::UNIX_EPOCH;
use tokio::runtime::Runtime;
use uuid::Uuid;

const LIBRARY_VERSION: &str = "1.3.14";

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
    let versioned_python_file_name = format!("xgb_connect_v{}.py", version.replace('.', "_"));
    let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

    // Remove old version files that do not match the current version
    for entry in read_dir(&exec_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        if file_name_str.starts_with("xgb_connect_v") && file_name_str != versioned_python_file_name
        {
            //println!("Removing old file: {}", file_name_str); // Debugging information
            remove_file(entry.path())?;
        }
    }

    // Check if the versioned Python file already exists
    if !versioned_python_file_path.exists() {
        // Write the Python script to the versioned file
        let mut out = BufWriter::new(File::create(&versioned_python_file_path)?);
        out.write_all(XGB_CONNECT_SCRIPT.as_bytes())?;
        out.flush()?;
    }

    Ok(())
}

/// Represents a pivot transformation on a dataset. This struct allows you to specify how to transform a dataset by pivoting one of its columns.
#[derive(Debug)]
pub struct XgbConfig {
    pub xgb_objective: String,
    pub xgb_max_depth: String,
    pub xgb_learning_rate: String,
    pub xgb_n_estimators: String,
    pub xgb_gamma: String,
    pub xgb_min_child_weight: String,
    pub xgb_subsample: String,
    pub xgb_colsample_bytree: String,
    pub xgb_reg_lambda: String,
    pub xgb_reg_alpha: String,
    pub xgb_scale_pos_weight: String,
    pub xgb_max_delta_step: String,
    pub xgb_booster: String,
    pub xgb_tree_method: String,
    pub xgb_grow_policy: String,
    pub xgb_eval_metric: String,
    pub xgb_early_stopping_rounds: String,
    pub xgb_device: String,
    pub xgb_cv: String,
    pub xgb_interaction_constraints: String,
    pub hyperparameter_optimization_attempts: String,
    pub hyperparameter_optimization_result_display_limit: String,
    pub dask_workers: String,
    pub dask_threads_per_worker: String,
}

pub struct XgbConnect;

impl XgbConnect {
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::xgb_utils::{XgbConfig, XgbConnect};
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
    /// let csv_path = current_dir.join("test_file_samples/xgb_test_files/xgb_regression_training_sample.csv");
    ///
    /// // Convert the path to a string
    /// let csv_path_str = csv_path.to_str().unwrap();
    /// // Append the relative path to the current directory
    /// let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models");
    ///
    /// // Convert the path to a string
    /// let model_dir_str = model_dir.to_str().unwrap();

    ///     let param_column_names = "no_of_tickets, last_60_days_tickets";
    ///     let target_column_name = "churn_day";
    ///     let prediction_column_name = "churn_day_PREDICTIONS";
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
    ///     dbg!(&csv_path_str, &param_column_names, &target_column_name, &model_dir_str, &model_name_str);
    ///
    ///     let result = XgbConnect::train(csv_path_str, param_column_names, target_column_name, prediction_column_name, model_dir_str, model_name_str, xgb_config).await;
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
    /// //assert_eq!(1,2);
    /// ```

    pub async fn train(
        csv_path_str: &str,
        param_column_names: &str,
        target_column_name: &str,
        prediction_column_name: &str,
        model_dir_str: &str,
        model_name_str: &str,
        xgb_config: XgbConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        /*
        let temp_path = task::block_in_place(|| {
            let rt = Runtime::new()?;
            rt.block_on(prepare_executable())
        })?;
        */
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name = format!("xgb_connect_v{}.py", version.replace('.', "_"));
        let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

        // Return the full command to execute the Python script
        let temp_path = format!("python3 {}", versioned_python_file_path.to_string_lossy());

        let formatted_param_column_names = format!("\"{}\"", param_column_names);

        // Initialize args with mandatory arguments
        let mut args = vec![
            "--csv_path".to_string(),
            csv_path_str.to_string(),
            "--params".to_string(),
            formatted_param_column_names.to_string(),
            "--target_column".to_string(),
            target_column_name.to_string(),
            "--prediction_column".to_string(),
            prediction_column_name.to_string(),
            "--model_dir".to_string(),
            model_dir_str.to_string(),
            "--model_name".to_string(),
            model_name_str.to_string(),
        ];

        // Helper macro to add optional arguments if they are not empty
        macro_rules! add_arg {
            ($flag:expr, $value:expr) => {
                if !$value.is_empty() {
                    args.push($flag.to_string());
                    args.push($value.to_string());
                }
            };
        }

        let uid = format!(
            "{}-{}",
            Uuid::new_v4().to_string(),
            Utc::now().timestamp_millis()
        );
        add_arg!("--uid", &uid);

        add_arg!("--objective", &xgb_config.xgb_objective);
        add_arg!("--max_depth", &xgb_config.xgb_max_depth);
        add_arg!("--learning_rate", &xgb_config.xgb_learning_rate);
        add_arg!("--n_estimators", &xgb_config.xgb_n_estimators);
        add_arg!("--gamma", &xgb_config.xgb_gamma);
        add_arg!("--min_child_weight", &xgb_config.xgb_min_child_weight);
        add_arg!("--subsample", &xgb_config.xgb_subsample);
        add_arg!("--colsample_bytree", &xgb_config.xgb_colsample_bytree);
        add_arg!("--reg_lambda", &xgb_config.xgb_reg_lambda);
        add_arg!("--reg_alpha", &xgb_config.xgb_reg_alpha);
        add_arg!("--scale_pos_weight", &xgb_config.xgb_scale_pos_weight);
        add_arg!("--max_delta_step", &xgb_config.xgb_max_delta_step);
        add_arg!("--booster", &xgb_config.xgb_booster);
        add_arg!("--tree_method", &xgb_config.xgb_tree_method);
        add_arg!("--grow_policy", &xgb_config.xgb_grow_policy);
        add_arg!("--eval_metric", &xgb_config.xgb_eval_metric);
        add_arg!(
            "--early_stopping_rounds",
            &xgb_config.xgb_early_stopping_rounds
        );
        add_arg!("--device", &xgb_config.xgb_device);
        add_arg!("--cv", &xgb_config.xgb_cv);
        add_arg!(
            "--interaction_constraints",
            &xgb_config.xgb_interaction_constraints
        );
        add_arg!(
            "--hyperparameter_optimization_attempts",
            &xgb_config.hyperparameter_optimization_attempts
        );
        add_arg!(
            "--hyperparameter_optimization_result_display_limit",
            &xgb_config.hyperparameter_optimization_result_display_limit
        );
        add_arg!("--dask_workers", &xgb_config.dask_workers);
        add_arg!(
            "--dask_threads_per_worker",
            &xgb_config.dask_threads_per_worker
        );

        //dbg!(&temp_path, &args);

        let command_str = format!("{} {}", temp_path, args.join(" "));
        //dbg!(&command_str);

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

                let filename = format!("rgwml_{}.json", &uid);
                let file = File::open(&filename)?;

                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

                //dbg!(&output_str);

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

                let report = json["report"]
                    .as_object()
                    .ok_or("Report missing in JSON")?
                    .iter()
                    .map(|(k, v)| vec![k.clone(), v.to_string()])
                    .collect::<Vec<Vec<String>>>();

                //dbg!(&headers, &rows, &report);
                remove_file(filename)?;

                Ok((headers, rows, report))
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// // Append the relative path to the current directory
    /// let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models");
    ///
    /// // Convert the path to a string
    /// let models_path = model_dir.to_str().unwrap();

    /// let mut csv_builder = XgbConnect::get_all_xgb_models(models_path).expect("Failed to load XGB models");
    ///
    /// csv_builder.print_table_all_rows();
    /// // Validate headers
    /// let headers = csv_builder.get_headers().expect("CsvBuilder should contain headers");
    /// assert_eq!(headers, &["model".to_string(), "last_modified".to_string(), "parameters".to_string(), "mb_size".to_string()]);
    ///
    /// // Validate data
    /// let data = csv_builder.get_data().expect("CsvBuilder should contain data");
    /// assert!(!data.is_empty(), "CsvBuilder data should not be empty");
    /// // Additional assertions can be added based on expected data
    /// ```

    pub fn get_all_xgb_models(path: &str) -> Result<CsvBuilder, Box<dyn Error>> {
        // Read the directory
        let dir_entries = read_dir(path)?;

        // Create a new CsvBuilder and set the headers
        let mut csv_builder = CsvBuilder::new();
        csv_builder.set_header(vec!["model", "last_modified", "parameters", "mb_size"]);

        // Iterate through each file in the directory
        for entry in dir_entries {
            let entry = entry?;
            let file_path = entry.path();

            // Only process JSON files
            if file_path.extension().map_or(false, |ext| ext == "json") {
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

                // Open the file and parse the JSON
                let file = File::open(&file_path)?;
                let reader = BufReader::new(file);
                let json_value: Value = serde_json::from_reader(reader)?;

                // Extract feature names
                if let Some(learner) = json_value.get("learner") {
                    if let Some(feature_names) = learner.get("feature_names") {
                        let feature_names_str = feature_names
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|f| f.as_str().unwrap().to_string())
                            .collect::<Vec<String>>()
                            .join(",");

                        let data_row = vec![
                            file_name,
                            formatted_timestamp,
                            feature_names_str,
                            format!("{:.2}", file_size_mb), // Add the file size in MB to the data row
                        ];

                        // Convert Vec<String> to Vec<&str>
                        let data_row_ref: Vec<&str> = data_row.iter().map(|s| s.as_str()).collect();

                        // Add the data row to the CsvBuilder
                        csv_builder.add_row(data_row_ref);
                    }
                }
            }
        }

        Ok(csv_builder)
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// // Append the relative path to the current directory
    /// let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models");
    ///
    /// // Append the file name to the directory path
    /// let model_path = model_dir.join("test_reg_model.json");
    ///
    /// // Convert the path to a string
    /// let model_path_str = model_path.to_str().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///     let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "flag".to_string()];
    ///     let data = vec![
    ///         vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "1".to_string()],
    ///         vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "0".to_string()],
    ///         vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "1".to_string()],
    ///         vec!["1".to_string(), "Charlie".to_string(), "28".to_string(), "2023-01-01 11:00:00".to_string(), "0".to_string()]
    ///     ];  
    ///     let mut builder = CsvBuilder::from_raw_data(headers, data);
    ///
    ///     // Append the relative path to the current directory
    ///     let csv_path = current_dir.join("test_file_samples/xgb_test_files/xgb_regression_training_sample.csv");
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let param_column_names = "no_of_tickets,last_60_days_tickets";
    ///     let prediction_column_name = "churn_day_PREDICTION";
    ///     
    ///     let result = XgbConnect::predict(csv_path_str, param_column_names, prediction_column_name, model_path_str).await;
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

    pub async fn predict(
        csv_path_str: &str,
        param_column_names: &str,
        prediction_column_name: &str,
        model_path_str: &str,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        /*
        let temp_path = task::block_in_place(|| {
            let rt = Runtime::new()?;
            rt.block_on(prepare_executable())
        })?;
        */
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name = format!("xgb_connect_v{}.py", version.replace('.', "_"));
        let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

        // Return the full command to execute the Python script
        let temp_path = format!("python3 {}", versioned_python_file_path.to_string_lossy());

        let formatted_param_column_names = format!("\"{}\"", param_column_names);

        let uid = format!(
            "{}-{}",
            Uuid::new_v4().to_string(),
            Utc::now().timestamp_millis()
        );

        // Initialize args with mandatory arguments
        let args = vec![
            "--uid".to_string(),
            uid.clone(),
            "--csv_path".to_string(),
            csv_path_str.to_string(),
            "--params".to_string(),
            formatted_param_column_names.to_string(),
            "--prediction_column".to_string(),
            prediction_column_name.to_string(),
            "--model_path".to_string(),
            model_path_str.to_string(),
        ];

        // dbg!(&temp_path, &args);

        let command_str = format!("{} {}", temp_path, args.join(" "));
        dbg!(&command_str);

        let output = Command::new("sh").arg("-c").arg(&command_str).output();

        match output {
            Ok(output) => {
                // dbg!(&output);
                if !output.status.success() {
                    let stderr = std::str::from_utf8(&output.stderr)?;
                    //eprintln!("Script error: {}", stderr);
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        stderr,
                    )));
                }

                //let output_str = std::str::from_utf8(&output.stdout)?;
                //println!("Output from the command: {}", output_str);

                //let file = File::open("output.json")?;

                let filename = format!("rgwml_{}.json", uid);
                let file = File::open(&filename)?;
                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;
                //dbg!(&output_str);

                /*
                    let json_str = output_str
                        .split("IGNORE_POINT")
                        .nth(1)
                        .ok_or("No JSON found in command output")?;

                    let json: Value = serde_json::from_str(json_str)
                        .map_err(|e| format!("Failed to parse JSON: {}", e))?;
                */
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

                //dbg!(&headers, &rows);
                remove_file(filename)?;
                Ok((headers, rows))
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                Err(Box::new(e))
            }
        }
    }
}
