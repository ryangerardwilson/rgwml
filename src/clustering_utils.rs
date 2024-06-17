// clustering_utils.rs

use crate::python_utils::CLUSTERING_CONNECT_SCRIPT;
use chrono::Utc;
use dirs;
use lazy_static::lazy_static;
use memmap::MmapOptions;
use serde_json::Value;
use std::fs::{create_dir_all, read_dir, remove_file, File};
use std::io::{BufWriter, Write};
use std::process::Command;
use tokio::runtime::Runtime;
use uuid::Uuid;

const LIBRARY_VERSION: &str = "1.3.03";

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
    let versioned_python_file_name =
        format!("clustering_connect_v{}.py", version.replace('.', "_"));
    let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

    // Remove old version files that do not match the current version
    for entry in read_dir(&exec_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        if file_name_str.starts_with("clustering_connect_v")
            && file_name_str != versioned_python_file_name
        {
            //println!("Removing old file: {}", file_name_str); // Debugging information
            remove_file(entry.path())?;
        }
    }

    // Check if the versioned Python file already exists
    if !versioned_python_file_path.exists() {
        // Write the Python script to the versioned file
        let mut out = BufWriter::new(File::create(&versioned_python_file_path)?);
        out.write_all(CLUSTERING_CONNECT_SCRIPT.as_bytes())?;
        out.flush()?;
    }

    Ok(())
}

/// Represents a pivot transformation on a dataset. This struct allows you to specify how to transform a dataset by pivoting one of its columns.
#[derive(Debug)]
pub struct ClusteringConfig {
    pub operation: String, //  Options: KMEANS, DBSCAN, AGGLOMERATIVE, MEAN_SHIFT, GMM, SPECTRAL, BIRCH
    pub optimal_n_cluster_finding_method: String, //  Options: FIXED:{n}, ELBOW, SILHOUETTE; Not relevant for DBSCAN and MEAN_SHIFT
    pub dbscan_eps: String,                       //  Only relevant for DBSCAN
    pub dbscan_min_samples: String,               //  Only relevant for DBSCAN
}

/// Represents a ClusteringConnect object
pub struct ClusteringConnect;

/// Implements ClusteringConnect
impl ClusteringConnect {
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::clustering_utils::{ClusteringConfig, ClusteringConnect};
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
    /// let csv_path = current_dir.join("test_file_samples/clustering_test_files/customers.csv");
    ///
    /// // Convert the path to a string
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    ///     let param_column_names = "age,annual_income,spending_score";
    ///     let cluster_column_name = "CLUSTERS";
    ///     
    ///     let clustering_config = ClusteringConfig {
    ///         operation: "KMEANS".to_string(),
    ///         optimal_n_cluster_finding_method: "ELBOW".to_string(),
    ///         dbscan_eps: "".to_string(),
    ///         dbscan_min_samples: "".to_string()
    ///     };
    ///
    ///     dbg!(&csv_path_str, &param_column_names, &cluster_column_name);
    ///
    ///     let result = ClusteringConnect::cluster(csv_path_str, param_column_names, cluster_column_name, clustering_config).await;
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

    pub async fn cluster(
        csv_path_str: &str,
        param_column_names: &str,
        cluster_column_name: &str,
        clustering_config: ClusteringConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name =
            format!("clustering_connect_v{}.py", version.replace('.', "_"));
        let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

        // Return the full command to execute the Python script
        let temp_path = format!("python3 {}", versioned_python_file_path.to_string_lossy());

        let formatted_param_column_names = format!("\"{}\"", param_column_names);

        // Initialize args with mandatory arguments
        let mut args = vec![
            "--csv_path".to_string(),
            csv_path_str.to_string(),
            "--features".to_string(),
            formatted_param_column_names.to_string(),
            "--cluster_column_name".to_string(),
            cluster_column_name.to_string(),
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

        add_arg!("--operation", &clustering_config.operation);
        add_arg!(
            "--optimal_n_cluster_finding_method",
            &clustering_config.optimal_n_cluster_finding_method
        );
        add_arg!("--dbscan_eps", &clustering_config.dbscan_eps);
        add_arg!(
            "--dbscan_min_samples",
            &clustering_config.dbscan_min_samples
        );

        //dbg!(&temp_path, &args);

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
                //println!("Output from the command: {}", output_str);
                //let file = File::open("output.json")?;

                let filename = format!("rgwml_{}.json", &uid);
                let file = File::open(&filename)?;

                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

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
}
