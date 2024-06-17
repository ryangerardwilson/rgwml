// dask_utils.rs
use crate::python_utils::{
    DASK_CLEANER_CONNECT_SCRIPT, DASK_DIFFERENTIATOR_CONNECT_SCRIPT,
    DASK_FREQ_CASCADING_CONNECT_SCRIPT, DASK_FREQ_LINEAR_CONNECT_SCRIPT,
    DASK_GROUPER_CONNECT_SCRIPT, DASK_INSPECT_CONNECT_SCRIPT, DASK_INTERSECTOR_CONNECT_SCRIPT,
    DASK_JOINER_CONNECT_SCRIPT, DASK_PIVOTER_CONNECT_SCRIPT,
    DASK_UNIQUE_VALUE_STATS_CONNECT_SCRIPT,
};
use chrono::Utc;
use dirs;
use lazy_static::lazy_static;
use memmap::MmapOptions;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, remove_file, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process::Command;
use tokio::runtime::Runtime;
use uuid::Uuid;

const LIBRARY_VERSION: &str = "1.3.66";

lazy_static! {
    static ref INITIALIZE: () = {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let home_dir = dirs::home_dir().unwrap();
            let rgwml_dir = home_dir.join("RGWML");
            let exec_dir = rgwml_dir.join("executables");

            let script_manager = DaskScriptManager::new(LIBRARY_VERSION, exec_dir);

            if let Err(e) = script_manager.prepare_executable().await {
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

#[async_trait::async_trait]
trait ScriptManager {
    async fn prepare_executable(&self) -> Result<(), Box<dyn std::error::Error>>;
}

struct DaskScriptManager {
    version: String,
    exec_dir: PathBuf,
}

impl DaskScriptManager {
    fn new(version: &str, exec_dir: PathBuf) -> Self {
        Self {
            version: version.replace('.', "_"),
            exec_dir,
        }
    }

    fn create_versioned_path(&self, script_name: &str) -> PathBuf {
        self.exec_dir
            .join(format!("{}_v{}.py", script_name, self.version))
    }

    fn remove_old_versions(
        &self,
        script_name: &str,
        current_versioned_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for entry in read_dir(&self.exec_dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            if file_name_str.starts_with(script_name) && file_name_str != current_versioned_name {
                remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    fn write_script_if_not_exists(
        &self,
        script_path: &PathBuf,
        script_content: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !script_path.exists() {
            let mut out = BufWriter::new(File::create(script_path)?);
            out.write_all(script_content.as_bytes())?;
            out.flush()?;
        }
        Ok(())
    }

    async fn prepare_script(
        &self,
        script_name: &str,
        script_content: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let versioned_script_path = self.create_versioned_path(script_name);
        self.remove_old_versions(script_name, &versioned_script_path.to_string_lossy())?;
        self.write_script_if_not_exists(&versioned_script_path, script_content)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ScriptManager for DaskScriptManager {
    async fn prepare_executable(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Create necessary directories
        if !self.exec_dir.exists() {
            create_dir_all(&self.exec_dir)?;
        }

        // Prepare each script
        self.prepare_script("dask_grouper_connect", DASK_GROUPER_CONNECT_SCRIPT)
            .await?;
        self.prepare_script("dask_pivoter_connect", DASK_PIVOTER_CONNECT_SCRIPT)
            .await?;
        self.prepare_script("dask_cleaner_connect", DASK_CLEANER_CONNECT_SCRIPT)
            .await?;
        self.prepare_script("dask_freq_linear_connect", DASK_FREQ_LINEAR_CONNECT_SCRIPT)
            .await?;
        self.prepare_script(
            "dask_freq_cascading_connect",
            DASK_FREQ_CASCADING_CONNECT_SCRIPT,
        )
        .await?;
        self.prepare_script("dask_inspect_connect", DASK_INSPECT_CONNECT_SCRIPT)
            .await?;
        self.prepare_script(
            "dask_unique_value_stats_connect",
            DASK_UNIQUE_VALUE_STATS_CONNECT_SCRIPT,
        )
        .await?;
        self.prepare_script("dask_joiner_connect", DASK_JOINER_CONNECT_SCRIPT)
            .await?;
        self.prepare_script("dask_intersector_connect", DASK_INTERSECTOR_CONNECT_SCRIPT)
            .await?;
        self.prepare_script(
            "dask_differentiator_connect",
            DASK_DIFFERENTIATOR_CONNECT_SCRIPT,
        )
        .await?;

        Ok(())
    }
}

/// Represents a pivot transformation on a dataset. This struct allows you to specify how to transform a dataset by pivoting one of its columns.
#[derive(Debug)]
pub struct DaskGrouperConfig {
    pub group_by_column_name: String,
    pub count_unique_agg_columns: String,
    pub numerical_max_agg_columns: String,
    pub numerical_min_agg_columns: String,
    pub numerical_sum_agg_columns: String,
    pub numerical_mean_agg_columns: String,
    pub numerical_median_agg_columns: String,
    pub numerical_std_deviation_agg_columns: String,
    pub mode_agg_columns: String,
    pub datetime_max_agg_columns: String,
    pub datetime_min_agg_columns: String,
    pub datetime_semi_colon_separated_agg_columns: String,
    pub bool_percent_agg_columns: String,
}

/// Represents a pivot transformation on a dataset. This struct allows you to specify how to transform a dataset by pivoting one of its columns.
#[derive(Debug)]
pub struct DaskPivoterConfig {
    pub group_by_column_name: String,
    pub values_to_aggregate_column_name: String,
    pub operation: String, // Options: COUNT, COUNT_UNIQUE, NUMERICAL_MAX, NUMERICAL_MIN, NUMERICAL_SUM, NUMERICAL_MEAN, NUMERICAL_MEDIAN, NUMERICAL_STANDARD_DEVIATION, BOOL_PERCENT
    pub segregate_by_column_names: String,
}

/// Represents a pivot transformation on a dataset. This struct allows you to specify how to transform a dataset by pivoting one of its columns.
#[derive(Debug, Clone)]
pub struct DaskCleanerConfig {
    pub rules: String,                         // Options:
    pub action: String,                        // Options: CLEAN, ANALYZE, ANALYZE_AND_CLEAN
    pub show_unclean_values_in_report: String, // Options: TRUE, FALSE
}

#[derive(Debug)]
pub struct DaskFreqLinearConfig {
    pub column_names: String,
    pub order_by: String, // Options: ASC, DESC, FREQ_ASC, FREQ_DESC
    pub limit: String,
}

#[derive(Debug)]
pub struct DaskFreqCascadingConfig {
    pub column_names: String,
    pub order_by: String, // Options: ASC, DESC, FREQ_ASC, FREQ_DESC
    pub limit: String,
}

#[derive(Debug, Clone)]
pub struct DaskJoinerConfig {
    pub join_type: String, // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
    pub table_a_ref_column: String,
    pub table_b_ref_column: String,
}

#[derive(Debug)]
pub struct DaskIntersectorConfig {
    pub table_a_ref_column: String,
    pub table_b_ref_column: String,
}

#[derive(Debug)]
pub struct DaskDifferentiatorConfig {
    pub difference_type: String, // Options: NORMAL, SYMMETRIC
    pub table_a_ref_column: String,
    pub table_b_ref_column: String,
}

/// Represents a ClusteringConnect object
pub struct DaskConnect;

/// Implements ClusteringConnect
impl DaskConnect {
    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskGrouperConfig, DaskConnect};
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
    /// let csv_path = current_dir.join("test_file_samples/grouping_test_files/customers.csv");
    ///
    /// // Convert the path to a string
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let dask_grouper_config = DaskGrouperConfig {
    ///     group_by_column_name: "age".to_string(),
    ///     count_unique_agg_columns: "".to_string(),
    ///     numerical_max_agg_columns: "annual_income".to_string(),
    ///     numerical_min_agg_columns: "annual_income".to_string(),
    ///     numerical_sum_agg_columns: "spending_score".to_string(),
    ///     numerical_mean_agg_columns: "loyalty_points".to_string(),
    ///     numerical_median_agg_columns: "".to_string(),
    ///     numerical_std_deviation_agg_columns: "".to_string(),
    ///     mode_agg_columns: "gender".to_string(),
    ///     datetime_max_agg_columns: "join_date".to_string(),
    ///     datetime_min_agg_columns: "join_date".to_string(),
    ///     datetime_semi_colon_separated_agg_columns: "".to_string(),
    ///     bool_percent_agg_columns: "".to_string(),
    /// };
    ///
    ///     let result = DaskConnect::dask_grouper(csv_path_str, dask_grouper_config).await;
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

    pub async fn dask_grouper(
        path: &str,
        dask_grouper_config: DaskGrouperConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name =
            format!("dask_grouper_connect_v{}.py", version.replace('.', "_"));
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

        add_arg!("--path", &path);
        add_arg!("--group_by", &dask_grouper_config.group_by_column_name);

        add_arg!(
            "--count_unique",
            &dask_grouper_config.count_unique_agg_columns
        );

        add_arg!(
            "--numerical_max",
            &dask_grouper_config.numerical_max_agg_columns
        );
        add_arg!(
            "--numerical_min",
            &dask_grouper_config.numerical_min_agg_columns
        );
        add_arg!(
            "--numerical_sum",
            &dask_grouper_config.numerical_sum_agg_columns
        );
        add_arg!(
            "--numerical_mean",
            &dask_grouper_config.numerical_mean_agg_columns
        );
        add_arg!(
            "--numerical_median",
            &dask_grouper_config.numerical_median_agg_columns
        );
        add_arg!(
            "--numerical_std",
            &dask_grouper_config.numerical_std_deviation_agg_columns
        );
        add_arg!("--mode", &dask_grouper_config.mode_agg_columns);
        add_arg!(
            "--datetime_max",
            &dask_grouper_config.datetime_max_agg_columns
        );
        add_arg!(
            "--datetime_min",
            &dask_grouper_config.datetime_min_agg_columns
        );
        add_arg!(
            "--datetime_semi_colon_separated",
            &dask_grouper_config.datetime_semi_colon_separated_agg_columns
        );
        add_arg!(
            "--bool_percent",
            &dask_grouper_config.bool_percent_agg_columns
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

                //let output_str = std::str::from_utf8(&output.stdout)?;
                //println!("Output from the command: {}", output_str);
                //let file = File::open("output.json")?;
                //
                //
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

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskPivoterConfig, DaskConnect};
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
    /// let csv_path = current_dir.join("test_file_samples/pivoting_test_files/employees.csv");
    ///
    /// // Convert the path to a string
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let dask_pivoter_config = DaskPivoterConfig {
    ///     group_by_column_name: "department".to_string(),
    ///     values_to_aggregate_column_name: "salary".to_string(),
    ///     operation: "NUMERICAL_MEAN".to_string(),
    ///     segregate_by_column_names: "is_manager, gender".to_string(),
    /// };
    ///
    ///     let result = DaskConnect::dask_pivoter(csv_path_str, dask_pivoter_config).await;
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

    pub async fn dask_pivoter(
        path: &str,
        dask_pivoter_config: DaskPivoterConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name =
            format!("dask_pivoter_connect_v{}.py", version.replace('.', "_"));
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

        add_arg!("--path", &path);
        add_arg!("--group_by", &dask_pivoter_config.group_by_column_name);

        add_arg!(
            "--values_from",
            &dask_pivoter_config.values_to_aggregate_column_name
        );

        add_arg!("--operation", &dask_pivoter_config.operation);
        add_arg!(
            "--segregate_by",
            &dask_pivoter_config.segregate_by_column_names
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

    /*

    #[derive(Debug)]
    pub struct DaskCleanerConfig {
        pub rules: String,
        pub action: String,  // Options: CLEAN, ANALYZE, ANALYZE_AND_CLEAN
        pub show_unclean_examples_in_report: String, // Options: TRUE, FALSE
    }



    */

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskCleanerConfig, DaskConnect};
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
    /// let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    /// // Convert the path to a string
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let dask_cleaner_config = DaskCleanerConfig {
    ///     rules: "mobile:IS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER".to_string(),
    ///     action: "ANALYZE_AND_CLEAN".to_string(),
    ///     show_unclean_values_in_report: "TRUE".to_string(),
    /// };
    ///
    /// let result = DaskConnect::dask_cleaner(csv_path_str, dask_cleaner_config).await;
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

    pub async fn dask_cleaner(
        path: &str,
        dask_cleaner_config: DaskCleanerConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>, HashMap<String, Value>), Box<dyn std::error::Error>>
    {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name =
            format!("dask_cleaner_connect_v{}.py", version.replace('.', "_"));
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

        add_arg!("--path", &path);
        add_arg!("--rules", &dask_cleaner_config.rules);

        add_arg!("--action", &dask_cleaner_config.action);

        add_arg!(
            "--show_unclean_values_in_report",
            &dask_cleaner_config.show_unclean_values_in_report
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

                let headers = json["headers"].as_array().map_or(Vec::new(), |arr| {
                    arr.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                });

                let rows = json["rows"].as_array().map_or(Vec::new(), |arr| {
                    arr.iter()
                        .map(|row| {
                            row.as_array()
                                .unwrap_or(&vec![])
                                .iter()
                                .map(|cell| cell.as_str().unwrap_or("").to_string())
                                .collect()
                        })
                        .collect()
                });

                /*
                let report = json["report"]
                    .as_object()
                    .ok_or("Report missing in JSON")?
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                */
                let report = json
                    .get("report")
                    .and_then(|r| r.as_object())
                    .map(|r| {
                        r.iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect::<HashMap<String, Value>>()
                    })
                    .unwrap_or_else(HashMap::new);
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
    /// use rgwml::dask_utils::DaskConnect;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///     // Append the file name to the directory path
    ///     let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let columns = "mobile, name";
    ///
    ///     let result = DaskConnect::dask_get_unique_value_stats(csv_path_str, columns).await;
    ///
    ///     match result {
    ///         Ok(res) => {
    ///             dbg!(&res);
    ///             // Assert that the result is a non-empty HashMap
    ///             assert!(!res.is_empty(), "Expected non-empty HashMap");
    ///
    ///             // Check for the 'name' key and its values
    ///             if let Some(name_stats) = res.get("name") {
    ///                 assert!(name_stats["total_unique_values"].is_number(), "Expected total_unique_values to be a number");
    ///                 assert!(name_stats["mean_frequency"].is_number(), "Expected mean_frequency to be a number");
    ///                 assert!(name_stats["median_frequency"].is_number(), "Expected median_frequency to be a number");
    ///             } else {
    ///                 assert!(false, "Expected 'name' key in the result");
    ///             }
    ///
    ///             // Check for the 'mobile' key and its values
    ///             if let Some(mobile_stats) = res.get("mobile") {
    ///                 assert!(mobile_stats["total_unique_values"].is_number(), "Expected total_unique_values to be a number");
    ///                 assert!(mobile_stats["mean_frequency"].is_number(), "Expected mean_frequency to be a number");
    ///                 assert!(mobile_stats["median_frequency"].is_number(), "Expected median_frequency to be a number");
    ///             } else {
    ///                 assert!(false, "Expected 'mobile' key in the result");
    ///             }
    ///         }
    ///         Err(e) => {
    ///             dbg!(e);
    ///             // Assert that the function should succeed, hence any error is unexpected
    ///             assert!(false, "Expected function to succeed but it failed");
    ///         }
    ///     }
    /// });
    /// ```

    pub async fn dask_get_unique_value_stats(
        path: &str,
        columns: &str,
    ) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name = format!(
            "dask_unique_value_stats_connect_v{}.py",
            version.replace('.', "_")
        );
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

        add_arg!("--path", &path);
        add_arg!("--columns", &columns);

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

                //let output_str = std::str::from_utf8(&output.stdout)?;
                //println!("Output from the command: {}", output_str);

                //let file = File::open("output.json")?;

                let filename = format!("rgwml_{}.json", &uid);
                let file = File::open(&filename)?;

                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

                let json: Value = serde_json::from_str(output_str)
                    .map_err(|e| format!("Failed to parse JSON: {}", e))?;

                let data: HashMap<String, Value> = serde_json::from_value(json)
                    .map_err(|e| format!("Failed to convert JSON to HashMap: {}", e))?;
                remove_file(filename)?;
                Ok(data)
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskFreqLinearConfig, DaskConnect};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///     // Append the file name to the directory path
    ///     let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let columns = "mobile, name";
    ///
    ///     let dask_freq_linear_config = DaskFreqLinearConfig {
    ///         column_names: columns.to_string(),
    ///         order_by: "FREQ_DESC".to_string(),
    ///         limit: "".to_string()
    ///     };
    ///
    ///     let result = DaskConnect::dask_get_freq_linear_stats(csv_path_str, dask_freq_linear_config).await;
    ///
    ///     match result {
    ///         Ok(res) => {
    ///             dbg!(&res);
    ///             // Assert that the result is a non-empty HashMap
    ///             assert!(!res.is_empty(), "Expected non-empty HashMap");
    ///
    ///             // Check for the 'unique_value_counts' key and its values
    ///             if let Some(unique_value_counts) = res.get("unique_value_counts") {
    ///                 if let Some(name_count) = unique_value_counts.get("name") {
    ///                     assert!(name_count.is_string(), "Expected name unique count to be a string");
    ///                 } else {
    ///                     assert!(false, "Expected 'name' key in unique_value_counts");
    ///                 }
    ///
    ///                 if let Some(mobile_count) = unique_value_counts.get("mobile") {
    ///                     assert!(mobile_count.is_string(), "Expected mobile unique count to be a string");
    ///                 } else {
    ///                     assert!(false, "Expected 'mobile' key in unique_value_counts");
    ///                 }
    ///             } else {
    ///                 assert!(false, "Expected 'unique_value_counts' key in the result");
    ///             }
    ///
    ///             // Check for the 'column_analysis' key and its values
    ///             if let Some(column_analysis) = res.get("column_analysis") {
    ///                 if let Some(name_stats) = column_analysis.get("name") {
    ///                     assert!(name_stats.is_object(), "Expected name stats to be an object");
    ///                     // Add further checks if needed, e.g., specific values in the object
    ///                 } else {
    ///                     assert!(false, "Expected 'name' key in column_analysis");
    ///                 }
    ///
    ///                 if let Some(mobile_stats) = column_analysis.get("mobile") {
    ///                     assert!(mobile_stats.is_object(), "Expected mobile stats to be an object");
    ///                     // Add further checks if needed, e.g., specific values in the object
    ///                 } else {
    ///                     assert!(false, "Expected 'mobile' key in column_analysis");
    ///                 }
    ///             } else {
    ///                 assert!(false, "Expected 'column_analysis' key in the result");
    ///             }
    ///         }
    ///         Err(e) => {
    ///             dbg!(e);
    ///             // Assert that the function should succeed, hence any error is unexpected
    ///             assert!(false, "Expected function to succeed but it failed");
    ///         }
    ///     }
    /// });
    /// ```

    pub async fn dask_get_freq_linear_stats(
        path: &str,
        dask_freq_linear_config: DaskFreqLinearConfig,
    ) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name =
            format!("dask_freq_linear_connect_v{}.py", version.replace('.', "_"));
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

        add_arg!("--path", &path);
        add_arg!("--columns", &dask_freq_linear_config.column_names);
        add_arg!("--limit", &dask_freq_linear_config.limit);
        add_arg!("--order_by", &dask_freq_linear_config.order_by);

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

                //let output_str = std::str::from_utf8(&output.stdout)?;
                //println!("Output from the command: {}", output_str);

                //let file = File::open("output.json")?;

                let filename = format!("rgwml_{}.json", &uid);
                let file = File::open(&filename)?;

                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

                let json: Value = serde_json::from_str(output_str)
                    .map_err(|e| format!("Failed to parse JSON: {}", e))?;

                let data: HashMap<String, Value> = serde_json::from_value(json)
                    .map_err(|e| format!("Failed to convert JSON to HashMap: {}", e))?;

                remove_file(filename)?;
                Ok(data)
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskFreqCascadingConfig, DaskConnect};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///     // Append the file name to the directory path
    ///     let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let columns = "mobile, name";
    ///
    ///     let dask_freq_cascading_config = DaskFreqCascadingConfig {
    ///         column_names: columns.to_string(),
    ///         order_by: "FREQ_DESC".to_string(),
    ///         limit: "".to_string()
    ///     };
    ///
    ///     let result = DaskConnect::dask_get_freq_cascading_stats(csv_path_str, dask_freq_cascading_config).await;
    ///
    ///     match result {
    ///         Ok(res) => {
    ///             dbg!(&res);
    ///             // Assert that the result is a non-empty HashMap
    ///             assert!(!res.is_empty(), "Expected non-empty HashMap");
    ///
    ///             // Check for each key and its values in the result
    ///             for (mobile_number, value) in res.iter() {
    ///                 assert!(value["count"].is_string(), "Expected count for '{}' to be a string", mobile_number);
    ///                 if let Some(sub_distribution) = value.get("sub_distribution(name)") {
    ///                     assert!(sub_distribution.is_object(), "Expected sub_distribution(name) for '{}' to be an object", mobile_number);
    ///                     for (name, sub_value) in sub_distribution.as_object().unwrap() {
    ///                         assert!(sub_value["count"].is_string(), "Expected count for '{}' in sub_distribution(name) to be a string", name);
    ///                     }
    ///                 }
    ///             }
    ///         }
    ///         Err(e) => {
    ///             dbg!(e);
    ///             // Assert that the function should succeed, hence any error is unexpected
    ///             assert!(false, "Expected function to succeed but it failed");
    ///         }
    ///     }
    /// });
    /// ```

    pub async fn dask_get_freq_cascading_stats(
        path: &str,
        dask_freq_cascading_config: DaskFreqCascadingConfig,
    ) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name = format!(
            "dask_freq_cascading_connect_v{}.py",
            version.replace('.', "_")
        );
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

        add_arg!("--path", &path);
        add_arg!("--columns", &dask_freq_cascading_config.column_names);
        add_arg!("--limit", &dask_freq_cascading_config.limit);
        add_arg!("--order_by", &dask_freq_cascading_config.order_by);

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

                //let output_str = std::str::from_utf8(&output.stdout)?;
                //println!("Output from the command: {}", output_str);

                //let file = File::open("output.json")?;

                let filename = format!("rgwml_{}.json", &uid);
                let file = File::open(&filename)?;

                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

                let json: Value = serde_json::from_str(output_str)
                    .map_err(|e| format!("Failed to parse JSON: {}", e))?;

                let data: HashMap<String, Value> = serde_json::from_value(json)
                    .map_err(|e| format!("Failed to convert JSON to HashMap: {}", e))?;

                remove_file(filename)?;
                Ok(data)
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::DaskConnect;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// // Get the current working directory
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///     // Append the file name to the directory path
    ///     let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let method = "GET_FIRST_ROW";   // GET_FIRST_ROW, GET_LAST_ROW, GET_FIRST_N_ROWS:X, GET_LAST_N_ROWS:Y, GET_ROW_RANGE:Z_A, GET_SUMMARY
    ///
    ///     let result = DaskConnect::dask_get_inspection_stats(csv_path_str, method).await;
    ///
    ///     match result {
    ///         Ok(res) => {
    ///             //dbg!(&res);
    ///             // Assert that the result is a non-empty HashMap
    ///             assert!(!res.is_empty(), "Expected non-empty HashMap");
    ///
    ///             // Check for the 'rows' key and its nested structure
    ///             if let Some(rows) = res.get("rows") {
    ///                 if let Some(first_row) = rows.get("1") {
    ///                     assert!(first_row.get("mobile").is_some(), "Expected 'mobile' field in the first row");
    ///                     assert!(first_row.get("name").is_some(), "Expected 'name' field in the first row");
    ///                     assert!(first_row.get("age").is_some(), "Expected 'age' field in the first row");
    ///                     // Add more field checks as necessary
    ///                 } else {
    ///                     assert!(false, "Expected '1' key in the rows");
    ///                 }
    ///             } else {
    ///                 assert!(false, "Expected 'rows' key in the result");
    ///             }
    ///
    ///         }
    ///         Err(e) => {
    ///             dbg!(e);
    ///             // Assert that the function should succeed, hence any error is unexpected
    ///             assert!(false, "Expected function to succeed but it failed");
    ///         }
    ///     }
    /// });
    /// ```

    pub async fn dask_get_inspection_stats(
        path: &str,
        method: &str,
    ) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name =
            format!("dask_inspect_connect_v{}.py", version.replace('.', "_"));
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

        add_arg!("--path", &path);
        add_arg!("--method", &method);

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

                //let output_str = std::str::from_utf8(&output.stdout)?;
                //println!("Output from the command: {}", output_str);

                //let file = File::open("output.json")?;

                let filename = format!("rgwml_{}.json", &uid);
                let file = File::open(&filename)?;

                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

                let json: Value = serde_json::from_str(output_str)
                    .map_err(|e| format!("Failed to parse JSON: {}", e))?;

                let data: HashMap<String, Value> = serde_json::from_value(json)
                    .map_err(|e| format!("Failed to convert JSON to HashMap: {}", e))?;

                remove_file(filename)?;
                Ok(data)
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskJoinerConfig, DaskConnect};
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
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let dask_joiner_config = DaskJoinerConfig {
    ///     join_type: "LEFT_JOIN".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let result = DaskConnect::dask_joiner(csv_path_a_str, csv_path_b_str, dask_joiner_config).await;
    ///
    ///     match result {
    ///         Ok(res) => {
    ///             //dbg!(&res);
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

    pub async fn dask_joiner(
        file_a_path: &str,
        file_b_path: &str,
        dask_joiner_config: DaskJoinerConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name =
            format!("dask_joiner_connect_v{}.py", version.replace('.', "_"));
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

        add_arg!("--file_a_path", &file_a_path);
        add_arg!("--file_b_path", &file_b_path);
        add_arg!("--join_type", &dask_joiner_config.join_type);
        add_arg!(
            "--file_a_ref_column",
            &dask_joiner_config.table_a_ref_column
        );
        add_arg!(
            "--file_b_ref_column",
            &dask_joiner_config.table_b_ref_column
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

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskIntersectorConfig, DaskConnect};
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
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let dask_intersector_config = DaskIntersectorConfig {
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let result = DaskConnect::dask_intersector(csv_path_a_str, csv_path_b_str, dask_intersector_config).await;
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

    pub async fn dask_intersector(
        file_a_path: &str,
        file_b_path: &str,
        dask_intersector_config: DaskIntersectorConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name =
            format!("dask_intersector_connect_v{}.py", version.replace('.', "_"));
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

        add_arg!("--file_a_path", &file_a_path);
        add_arg!("--file_b_path", &file_b_path);
        add_arg!(
            "--file_a_ref_column",
            &dask_intersector_config.table_a_ref_column
        );
        add_arg!(
            "--file_b_ref_column",
            &dask_intersector_config.table_b_ref_column
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

    /// ```
    /// use rgwml::csv_utils::CsvBuilder;
    /// use rgwml::dask_utils::{DaskDifferentiatorConfig, DaskConnect};
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
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let dask_differentiator_config = DaskDifferentiatorConfig {
    ///     difference_type: "NORMAL".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let result = DaskConnect::dask_differentiator(csv_path_a_str, csv_path_b_str, dask_differentiator_config).await;
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

    pub async fn dask_differentiator(
        file_a_path: &str,
        file_b_path: &str,
        dask_differentiator_config: DaskDifferentiatorConfig,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name = format!(
            "dask_differentiator_connect_v{}.py",
            version.replace('.', "_")
        );
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

        add_arg!("--file_a_path", &file_a_path);
        add_arg!("--file_b_path", &file_b_path);
        add_arg!(
            "--difference_type",
            &dask_differentiator_config.difference_type
        );
        add_arg!(
            "--file_a_ref_column",
            &dask_differentiator_config.table_a_ref_column
        );
        add_arg!(
            "--file_b_ref_column",
            &dask_differentiator_config.table_b_ref_column
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
