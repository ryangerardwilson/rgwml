// src/heavy_csv_utils.rs
use crate::dask_utils::{
    DaskCleanerConfig, DaskConnect, DaskDifferentiatorConfig, DaskFreqCascadingConfig,
    DaskFreqLinearConfig, DaskGrouperConfig, DaskIntersectorConfig, DaskJoinerConfig,
    DaskPivoterConfig,
};
use crate::db_utils::DbConnect;
use crate::dc_utils::{DataContainer, DcConnectConfig};
use crate::public_url_utils::{PublicUrlConnect, PublicUrlConnectConfig};
use calamine::{open_workbook, Reader, Xls, Xlsx};
use chrono::Utc;
use memmap::MmapOptions;
use serde_json::Value;
use std::error::Error;
use std::fs::{remove_file, File};
use std::io::Read;
use std::io::Write;
use std::process::Command;
use tempfile::NamedTempFile;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct HeavyCsvBuilder {
    data: Vec<Vec<u8>>,
    headers: Vec<String>,
}

impl HeavyCsvBuilder {
    /// Creates a new, empty `HeavyCsvBuilder`.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let builder = HeavyCsvBuilder::heavy_new();
    ///
    /// // Initially, there are no headers or data
    /// assert!(builder.heavy_get_headers().is_empty());
    /// assert!(builder.heavy_get_data().is_empty());
    /// ```
    pub fn heavy_new() -> Self {
        HeavyCsvBuilder {
            headers: Vec::new(),
            data: Vec::new(),
        }
    }

    /// Creates a new `HeavyCsvBuilder` from a copy of an existing one.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let original = HeavyCsvBuilder::heavy_new();
    /// let copy = original.heavy_from_copy();
    ///
    /// // The copy should have the same headers and data as the original
    /// assert_eq!(original.heavy_get_headers(), copy.heavy_get_headers());
    /// assert_eq!(original.heavy_get_data(), copy.heavy_get_data());
    /// ```
    pub fn heavy_from_copy(&self) -> Self {
        HeavyCsvBuilder {
            headers: self.headers.clone(),
            data: self.data.clone(),
        }
    }

    /*
    /// Returns a range of rows formatted as JSON-like strings with headers.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use tokio::runtime::Runtime;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///     let args = vec![
    ///         ("--start_date", "2024-06-03"),
    ///         ("--end_date", "2024-06-04"),
    ///         ("--delta_file_dir", "/home/rgw/Desktop/temp/ticketing_and_usage_data_churn_analysis/h5_files"),
    ///         ("--google_application_credentials_path", "/home/rgw/Apps/rgw/tokens/google-big-query-credentials.json")
    ///     ];
    ///
    ///     let mut builder = HeavyCsvBuilder::heavy_from_bare_metal_python_executable(
    ///         "/home/rgw/Apps/rgw_analytics/python_executables/get_i2e1_usage_data.py",
    ///         args,
    ///     ).await;
    ///
    ///     let result_str = builder.heavy_print_table().await;
    ///     assert_eq!(1, 2);
    /// });
    /// ```
     */

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let executable_path = current_dir.join("python_executables/dask_joiner_connect.py");
    ///
    /// let executable_path_str = executable_path.to_str().unwrap();
    ///
    /// // Append the file name to the directory path
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///     let args = vec![
    ///         ("--file_a_path", csv_path_a_str),
    ///         ("--file_b_path", csv_path_b_str),
    ///         ("--join_type", "LEFT_JOIN"),
    ///         ("--file_a_ref_column", "id"),
    ///         ("--file_b_ref_column", "id")
    ///     ];
    ///
    ///     let mut builder = HeavyCsvBuilder::heavy_from_bare_metal_python_executable(
    ///         &executable_path_str,
    ///         args,
    ///     ).await;
    ///
    ///     let result_str = builder.heavy_print_table().await;
    ///     assert_eq!(builder.heavy_has_data(), true);
    /// });
    /// ```

    pub async fn heavy_from_bare_metal_python_executable(
        script_path: &str,
        args: Vec<(&str, &str)>,
    ) -> Self {
        let uid = format!(
            "{}-{}",
            Uuid::new_v4().to_string(),
            Utc::now().timestamp_millis()
        );
        let temp_filename = format!("rgwml_{}.json", &uid);

        let mut args_flat = vec![format!("--uid {}", uid)];
        for (flag, value) in &args {
            args_flat.push(format!("{} {}", flag, value));
        }

        let command_str = format!("python3 {} {}", script_path, args_flat.join(" "));

        dbg!(&command_str);
        let output = Command::new("sh").arg("-c").arg(&command_str).output();

        match output {
            Ok(output) => {
                if !output.status.success() {
                    let stderr = std::str::from_utf8(&output.stderr).unwrap_or("");
                    eprintln!("Script error: {}", stderr);
                    return HeavyCsvBuilder {
                        data: vec![],
                        headers: vec![],
                    };
                }

                let file = File::open(&temp_filename).unwrap();
                let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
                let output_str = std::str::from_utf8(&mmap).unwrap();
                //dbg!(&output_str);
                let json: Value = serde_json::from_str(output_str).unwrap_or_else(|e| {
                    panic!("Failed to parse JSON: {}", e);
                });

                let headers = json["headers"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|v| v.as_str().unwrap_or("").to_string())
                    .collect();

                let rows = json["rows"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|row| {
                        row.as_array()
                            .unwrap_or(&vec![])
                            .iter()
                            .map(|cell| cell.as_str().unwrap_or("").to_string())
                            .collect()
                    })
                    .collect();

                remove_file(temp_filename).unwrap();

                HeavyCsvBuilder::heavy_from_raw_data(headers, rows)
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                HeavyCsvBuilder {
                    data: vec![],
                    headers: vec![],
                }
            }
        }
    }

    /// Creates a `HeavyCsvBuilder` from a file.
    ///
    /// # Arguments
    ///
    /// * `file_path` - A string slice that holds the path to the CSV file.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use std::error::Error;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    /// let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let csv_builder = HeavyCsvBuilder::heavy_from_csv(csv_path_str);
    ///
    /// let headers = csv_builder.heavy_get_headers();
    /// dbg!(&headers);
    /// assert_eq!(csv_builder.heavy_get_headers(), &vec!["mobile".to_string(), "name".to_string(), "age".to_string()]);
    /// ```
    /// # Errors
    ///
    /// This function will return an error if the file cannot be read or if there is an issue
    /// with parsing the file.
    pub fn heavy_from_csv(file_path: &str) -> Self {
        let delimiter = b',';
        let mut file = File::open(file_path).expect("Failed to open file");
        let mut data = Vec::new();
        file.read_to_end(&mut data).expect("Failed to read file");

        // Assuming the first line contains headers
        let first_line_end = data.iter().position(|&b| b == b'\n').unwrap_or(data.len());
        let headers_line = &data[..first_line_end];
        let headers = headers_line
            .split(|&b| b == delimiter)
            .map(|s| String::from_utf8_lossy(s).to_string())
            .collect();

        let data_start = first_line_end + 1; // Skip the newline character
        let data_lines = data[data_start..]
            .split(|&b| b == b'\n')
            .map(|line| line.to_vec())
            .collect::<Vec<Vec<u8>>>(); // Store each line as a Vec<u8>

        HeavyCsvBuilder {
            data: data_lines,
            headers,
        }
    }

    /// Creates a `HeavyCsvBuilder` instance from headers and data.
    ///
    /// ```                 
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    /// ];
    ///
    /// let heavy_csv_builder = HeavyCsvBuilder::heavy_from_raw_data(headers.clone(), data.clone());
    ///
    /// // Check that the headers are correct
    /// assert_eq!(heavy_csv_builder.heavy_get_headers(), &headers);
    ///
    /// // Check that the data is correct
    /// let data = heavy_csv_builder.heavy_get_data_as_string().unwrap();
    /// let expected_data = "1,Alice,30\n2,Bob,25\n";
    /// assert_eq!(data, expected_data);
    /// ```
    pub fn heavy_from_raw_data(headers: Vec<String>, data: Vec<Vec<String>>) -> Self {
        let mut data_bytes: Vec<Vec<u8>> = Vec::new();
        let delimiter = b',';

        // Write data rows
        for row in data {
            let row_bytes = row.join(&(delimiter as char).to_string()).into_bytes();
            data_bytes.push(row_bytes);
        }

        HeavyCsvBuilder {
            data: data_bytes,
            headers,
        }
    }

    pub async fn heavy_from_publicly_viewable_google_sheet(url: &str) -> Self {
        let mut builder = HeavyCsvBuilder::heavy_new();

        let public_url_connect_config = PublicUrlConnectConfig {
            url: url.to_string(),
            url_type: "GOOGLE_SHEETS".to_string(),
        };

        match PublicUrlConnect::get_google_sheets_data(public_url_connect_config).await {
            Ok((headers, rows)) => {
                builder.headers = headers;
                builder.data = rows
                    .into_iter()
                    .map(|row| row.join(",").into_bytes())
                    .collect();
            }
            Err(e) => {
                //builder.error = Some(e);
                println!("Error fetching data: {}", e);
            }
        }

        builder
    }

    pub fn heavy_from_xls(file_path: &str, sheet_identifier: &str, identifier_type: &str) -> Self {
        let mut builder = HeavyCsvBuilder::heavy_new();

        match open_workbook::<Xls<_>, _>(file_path) {
            Ok(mut workbook) => {
                let sheet_names = workbook.sheet_names();
                let sheet_name_opt = match identifier_type {
                    "SHEET_NAME" => Some(sheet_identifier.to_string()),
                    "SHEET_ID" => {
                        if let Ok(index) = sheet_identifier.parse::<usize>() {
                            if index > 0 && index <= sheet_names.len() {
                                Some(sheet_names[index - 1].clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                match sheet_name_opt {
                    Some(sheet_name) => match workbook.worksheet_range(&sheet_name) {
                        Ok(range) => {
                            for row in range.rows() {
                                let row_data: Vec<String> =
                                    row.iter().map(|cell| cell.to_string()).collect();
                                if builder.headers.is_empty() {
                                    builder.headers = row_data;
                                } else {
                                    builder.data.push(row_data.join(",").into_bytes());
                                }
                            }
                        }
                        Err(_e) => {
                            //let error = Box::new(e) as Box<dyn Error>;
                            //builder.error = Some(error);
                            println!("Error fetching data");
                        }
                    },
                    None => {
                        println!("Error fetching data");
                    }
                }
            }
            Err(_e) => {
                //builder.error = Some(error);
                println!("Error fetching data");
            }
        }

        builder
    }

    pub fn heavy_from_xlsx(file_path: &str, sheet_identifier: &str, identifier_type: &str) -> Self {
        let mut builder = HeavyCsvBuilder::heavy_new();

        match open_workbook::<Xlsx<_>, _>(file_path) {
            Ok(mut workbook) => {
                let sheet_names = workbook.sheet_names();
                let sheet_name_opt = match identifier_type {
                    "SHEET_NAME" => Some(sheet_identifier.to_string()),
                    "SHEET_ID" => {
                        if let Ok(index) = sheet_identifier.parse::<usize>() {
                            if index > 0 && index <= sheet_names.len() {
                                Some(sheet_names[index - 1].clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                match sheet_name_opt {
                    Some(sheet_name) => match workbook.worksheet_range(&sheet_name) {
                        Ok(range) => {
                            for row in range.rows() {
                                let row_data: Vec<String> =
                                    row.iter().map(|cell| cell.to_string()).collect();
                                if builder.headers.is_empty() {
                                    builder.headers = row_data;
                                } else {
                                    builder.data.push(row_data.join(",").into_bytes());
                                }
                            }
                        }
                        Err(_e) => {
                            println!("Error fetching data");
                        }
                    },
                    None => {
                        //builder.error = Some(Box::new(error) as Box<dyn Error>);
                        println!("Error fetching data");
                    }
                }
            }
            Err(_e) => {
                //builder.error = Some(error);
                println!("Error fetching data");
            }
        }

        builder
    }

    pub async fn heavy_from_h5(
        file_path: &str,
        dataset_identifier: &str,
        identifier_type: &str,
    ) -> Self {
        let mut builder = HeavyCsvBuilder::heavy_new();

        let dc_connect_config = DcConnectConfig {
            path: file_path.to_string(),
            dc_type: "H5".to_string(),
            h5_dataset_identifier: dataset_identifier.to_string(),
            h5_identifier_type: identifier_type.to_string(),
        };

        match DataContainer::get_dc_data(dc_connect_config).await {
            Ok((headers, rows)) => {
                builder.headers = headers;
                builder.data = rows
                    .into_iter()
                    .map(|row| row.join(",").into_bytes())
                    .collect();
            }
            Err(_e) => {
                //builder.error = Some(e);
                println!("Error fetching data");
            }
        }

        builder
    }

    pub async fn heavy_from_mssql_query(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let result =
            DbConnect::execute_mssql_query(username, password, server, database, sql_query).await?;

        Ok(HeavyCsvBuilder::heavy_from_raw_data(result.0, result.1))
    }

    pub async fn heavy_from_chunked_mssql_query_union(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<HeavyCsvBuilder> = None;
        let dask_joiner_config = DaskJoinerConfig {
            join_type: "UNION".to_string(),
            table_a_ref_column: "".to_string(),
            table_b_ref_column: "".to_string(),
        };

        loop {
            // Construct the chunked query using OFFSET and FETCH NEXT for MSSQL
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery ORDER BY (SELECT NULL) OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                sql_query, offset, chunk_size
            );

            chunk_query.retain(|c| c != ';');
            // Execute the chunked query
            let result = HeavyCsvBuilder::heavy_from_mssql_query(
                username,
                password,
                server,
                database,
                &chunk_query,
            )
            .await?;

            // If the chunk has no data, break the loop
            if result.data.is_empty() {
                break;
            }

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .heavy_union_with_csv_builder_big(result, dask_joiner_config.clone())
                        .await;
                }
                None => {
                    combined_builder = Some(result);
                }
            }

            // Update offset for the next chunk
            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    pub async fn heavy_from_chunked_mssql_query_bag_union(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<HeavyCsvBuilder> = None;
        let dask_joiner_config = DaskJoinerConfig {
            join_type: "BAG_UNION".to_string(),
            table_a_ref_column: "".to_string(),
            table_b_ref_column: "".to_string(),
        };

        loop {
            // Construct the chunked query using OFFSET and FETCH NEXT for MSSQL
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery ORDER BY (SELECT NULL) OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                sql_query, offset, chunk_size
            );

            chunk_query.retain(|c| c != ';');
            // Execute the chunked query
            let result = HeavyCsvBuilder::heavy_from_mssql_query(
                username,
                password,
                server,
                database,
                &chunk_query,
            )
            .await?;

            // If the chunk has no data, break the loop
            if result.data.is_empty() {
                break;
            }

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .heavy_union_with_csv_builder_big(result, dask_joiner_config.clone())
                        .await;
                }
                None => {
                    combined_builder = Some(result);
                }
            }

            // Update offset for the next chunk
            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `HeavyCsvBuilder` instance directly from a MySQL query.
    pub async fn heavy_from_mysql_query(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let result =
            DbConnect::execute_mysql_query(username, password, server, database, sql_query).await?;

        Ok(HeavyCsvBuilder::heavy_from_raw_data(result.0, result.1))
    }

    /// Creates a `HeavyCsvBuilder` instance directly from a MySQL query, receiving the data in chunks.
    pub async fn heavy_from_chunked_mysql_query_union(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<HeavyCsvBuilder> = None;
        let dask_joiner_config = DaskJoinerConfig {
            join_type: "UNION".to_string(),
            table_a_ref_column: "".to_string(),
            table_b_ref_column: "".to_string(),
        };

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            chunk_query.retain(|c| c != ';');
            let result = HeavyCsvBuilder::heavy_from_mysql_query(
                username,
                password,
                server,
                database,
                &chunk_query,
            )
            .await?;

            if result.data.is_empty() {
                break;
            }

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .heavy_union_with_csv_builder_big(result, dask_joiner_config.clone())
                        .await;
                }
                None => {
                    combined_builder = Some(result);
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    pub async fn heavy_from_chunked_mysql_query_bag_union(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<HeavyCsvBuilder> = None;
        let dask_joiner_config = DaskJoinerConfig {
            join_type: "BAG_UNION".to_string(),
            table_a_ref_column: "".to_string(),
            table_b_ref_column: "".to_string(),
        };

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            chunk_query.retain(|c| c != ';');
            let result = HeavyCsvBuilder::heavy_from_mysql_query(
                username,
                password,
                server,
                database,
                &chunk_query,
            )
            .await?;

            if result.data.is_empty() {
                break;
            }

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .heavy_union_with_csv_builder_big(result, dask_joiner_config.clone())
                        .await;
                }
                None => {
                    combined_builder = Some(result);
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `HeavyCsvBuilder` instance directly from a ClickHouse query.
    pub async fn heavy_from_clickhouse_query(
        username: &str,
        password: &str,
        server: &str,
        sql_query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let result =
            DbConnect::execute_clickhouse_query(username, password, server, sql_query).await?;

        Ok(HeavyCsvBuilder::heavy_from_raw_data(result.0, result.1))
    }

    /// Creates a `HeavyCsvBuilder` instance directly from a ClickHouse query, receiving the data in chunks.
    pub async fn heavy_from_chunked_clickhouse_query_union(
        username: &str,
        password: &str,
        server: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<HeavyCsvBuilder> = None;
        let dask_joiner_config = DaskJoinerConfig {
            join_type: "UNION".to_string(),
            table_a_ref_column: "".to_string(),
            table_b_ref_column: "".to_string(),
        };

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            chunk_query.retain(|c| c != ';');
            let result = HeavyCsvBuilder::heavy_from_clickhouse_query(
                username,
                password,
                server,
                &chunk_query,
            )
            .await?;

            if result.data.is_empty() {
                break;
            }

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .heavy_union_with_csv_builder_big(result, dask_joiner_config.clone())
                        .await;
                }
                None => {
                    combined_builder = Some(result);
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `HeavyCsvBuilder` instance directly from a ClickHouse query, receiving the data in chunks,
    /// as a bag union.
    pub async fn heavy_from_chunked_clickhouse_query_bag_union(
        username: &str,
        password: &str,
        server: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<HeavyCsvBuilder> = None;
        let dask_joiner_config = DaskJoinerConfig {
            join_type: "BAG_UNION".to_string(),
            table_a_ref_column: "".to_string(),
            table_b_ref_column: "".to_string(),
        };

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            chunk_query.retain(|c| c != ';');
            let result = HeavyCsvBuilder::heavy_from_clickhouse_query(
                username,
                password,
                server,
                &chunk_query,
            )
            .await?;

            if result.data.is_empty() {
                break;
            }

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .heavy_union_with_csv_builder_big(result, dask_joiner_config.clone())
                        .await;
                }
                None => {
                    combined_builder = Some(result);
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `HeavyCsvBuilder` instance directly from a Google BigQuery query.
    pub async fn heavy_from_google_big_query_query(
        json_credentials_path: &str,
        sql_query: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let result =
            DbConnect::execute_google_big_query_query(json_credentials_path, sql_query).await?;

        Ok(HeavyCsvBuilder::heavy_from_raw_data(result.0, result.1))
    }

    /// Creates a `HeavyCsvBuilder` instance directly from a Google BigQuery query, receiving the data in chunks.
    pub async fn heavy_from_chunked_google_big_query_query_union(
        json_credentials_path: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<HeavyCsvBuilder> = None;
        let dask_joiner_config = DaskJoinerConfig {
            join_type: "UNION".to_string(),
            table_a_ref_column: "".to_string(),
            table_b_ref_column: "".to_string(),
        };

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            chunk_query.retain(|c| c != ';');
            let result = HeavyCsvBuilder::heavy_from_google_big_query_query(
                json_credentials_path,
                &chunk_query,
            )
            .await?;

            if result.data.is_empty() {
                break;
            }

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .heavy_union_with_csv_builder_big(result, dask_joiner_config.clone())
                        .await;
                }
                None => {
                    combined_builder = Some(result);
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    /// Creates a `HeavyCsvBuilder` instance directly from a Google BigQuery query, receiving the data in chunks,
    /// as a bag union.
    pub async fn heavy_from_chunked_google_big_query_query_bag_union(
        json_credentials_path: &str,
        sql_query: &str,
        chunk_size: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let chunk_size: usize = chunk_size
            .parse()
            .map_err(|_| "Invalid chunk size format")?;

        let mut offset = 0;
        let mut combined_builder: Option<HeavyCsvBuilder> = None;
        let dask_joiner_config = DaskJoinerConfig {
            join_type: "BAG_UNION".to_string(),
            table_a_ref_column: "".to_string(),
            table_b_ref_column: "".to_string(),
        };

        loop {
            let mut chunk_query = format!(
                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                sql_query, chunk_size, offset
            );

            chunk_query.retain(|c| c != ';');
            let result = HeavyCsvBuilder::heavy_from_google_big_query_query(
                json_credentials_path,
                &chunk_query,
            )
            .await?;

            if result.data.is_empty() {
                break;
            }

            match &mut combined_builder {
                Some(builder) => {
                    builder
                        .heavy_union_with_csv_builder_big(result, dask_joiner_config.clone())
                        .await;
                }
                None => {
                    combined_builder = Some(result);
                }
            }

            offset += chunk_size;
        }

        combined_builder.ok_or_else(|| "No data fetched".into())
    }

    pub fn heavy_save_as(&mut self, new_file_path: &str) -> Result<&mut Self, Box<dyn Error>> {
        let file = File::create(new_file_path)?;
        let mut wtr = csv::Writer::from_writer(file);

        // Write the headers
        if !self.headers.is_empty() {
            wtr.write_record(&self.headers)?;
        }

        // Ensure each data row has the same number of elements as there are headers
        let headers_len = self.headers.len();
        for record in &mut self.data {
            let record_str = String::from_utf8_lossy(record);
            let mut record_vec: Vec<&str> = record_str.split(',').collect();

            // Pad the record with empty strings if it has fewer elements than headers
            while record_vec.len() < headers_len {
                record_vec.push("");
            }

            wtr.write_record(&record_vec)?;
        }

        wtr.flush()?;

        Ok(self)
    }

    /// Returns a reference to the headers.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_new();
    /// builder.heavy_set_headers(vec!["header1".to_string(), "header2".to_string()]);
    /// assert_eq!(builder.heavy_get_headers(), &vec!["header1".to_string(), "header2".to_string()]);
    /// ```
    pub fn heavy_set_headers(&mut self, headers: Vec<String>) {
        self.headers = headers;
    }

    /// Returns a reference to the headers.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_new();
    /// builder.heavy_set_headers(vec!["header1".to_string(), "header2".to_string()]);
    /// assert_eq!(builder.heavy_get_headers(), &vec!["header1".to_string(), "header2".to_string()]);
    /// ```
    pub fn heavy_get_headers(&self) -> &Vec<String> {
        &self.headers
    }

    /// Returns the raw data as a concatenated vector of bytes.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_new();
    /// builder.heavy_set_data(vec![b"data1,data2".to_vec()]);
    /// assert_eq!(builder.heavy_get_data(), b"data1,data2".to_vec());
    /// ```

    pub fn heavy_get_data(&self) -> Vec<u8> {
        self.data.concat()
    }

    /// Sets the raw data for the `HeavyCsvBuilder`.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_new();
    /// builder.heavy_set_data(vec![b"data1,data2".to_vec()]);
    /// assert_eq!(builder.heavy_get_data(), b"data1,data2".to_vec());
    /// ```
    pub fn heavy_set_data(&mut self, data: Vec<Vec<u8>>) {
        self.data = data;
    }

    /// Returns the raw data as a single concatenated string.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use std::error::Error;
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_new();
    /// builder.heavy_set_data(vec![b"data1,data2".to_vec()]);
    /// let result = builder.heavy_get_data_as_string();
    /// assert!(result.is_ok());
    /// assert_eq!(result.unwrap(), "data1,data2\n");
    /// ```
    pub fn heavy_get_data_as_string(&self) -> Result<String, Box<dyn Error>> {
        let mut concatenated_data = String::new();

        for row in &self.data {
            let row_str = String::from_utf8(row.clone())?;
            concatenated_data.push_str(&row_str);
            concatenated_data.push('\n');
        }

        Ok(concatenated_data)
    }

    /// Parses a row into a vector of strings.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_new();
    /// builder.heavy_set_data(vec![b"data1,data2".to_vec()]);
    /// assert_eq!(builder.heavy_get_row_as_vector_of_strings(0), Some(vec!["data1".to_string(), "data2".to_string()]));     
    /// assert_eq!(builder.heavy_get_row_as_vector_of_strings(1), None);
    /// ```

    pub fn heavy_get_row_as_vector_of_strings(&self, row_number: usize) -> Option<Vec<String>> {
        let delimiter = b',';
        self.data.get(row_number).map(|row| {
            row.split(|&b| b == delimiter)
                .map(|cell| String::from_utf8_lossy(cell).to_string())
                .collect()
        })
    }

    /// Returns the data as a vector of vectors of strings.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    /// ];
    ///
    /// let builder = HeavyCsvBuilder::heavy_from_raw_data(headers.clone(), data.clone());
    ///
    /// let result = builder.heavy_get_data_as_vector_of_vector_strings();
    ///
    /// let expected_result = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    /// ];
    ///
    /// assert_eq!(result, expected_result);
    /// ```
    pub fn heavy_get_data_as_vector_of_vector_strings(&self) -> Vec<Vec<String>> {
        (0..self.data.len())
            .filter_map(|i| self.heavy_get_row_as_vector_of_strings(i))
            .collect()
    }

    /// Checks if the Heavy CSV builder contains any data (either headers or rows).
    ///
    /// # Returns
    /// Returns `true` if there is any data in the headers or rows, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let builder = HeavyCsvBuilder::heavy_from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![vec!["2023-01-30".to_string(), "23.5".to_string()]]
    /// );
    ///
    /// assert!(builder.heavy_has_data());
    /// ```
    pub fn heavy_has_data(&self) -> bool {
        !self.headers.is_empty() || !self.data.is_empty()
    }

    /// Checks if the Heavy CSV builder contains headers.
    ///
    /// # Returns
    /// Returns `true` if headers are present, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    ///
    /// let builder = HeavyCsvBuilder::heavy_from_raw_data(
    ///     vec!["date".to_string(), "temperature".to_string()],
    ///     vec![]
    /// );
    ///
    /// assert!(builder.heavy_has_headers());
    /// ```
    pub fn heavy_has_headers(&self) -> bool {
        !self.headers.is_empty()
    }

    /// Prints the first row of the CSV data.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string(), "email".to_string(), "phone".to_string(), "address".to_string(), "city".to_string(), "state".to_string(), "country".to_string(), "zipcode".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "alice@example.com".to_string(), "123-456-7890".to_string(), "123 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62701".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string(), "bob@example.com".to_string(), "123-456-7891".to_string(), "124 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62702".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string(), "charlie@example.com".to_string(), "123-456-7892".to_string(), "125 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62703".to_string()],
    ///     vec!["4".to_string(), "David".to_string(), "28".to_string(), "david@example.com".to_string(), "123-456-7893".to_string(), "126 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62704".to_string()],
    ///     vec!["5".to_string(), "Eve".to_string(), "22".to_string(), "eve@example.com".to_string(), "123-456-7894".to_string(), "127 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62705".to_string()],
    ///     vec!["6".to_string(), "Frank".to_string(), "33".to_string(), "frank@example.com".to_string(), "123-456-7895".to_string(), "128 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62706".to_string()],
    ///     vec!["7".to_string(), "Grace".to_string(), "27".to_string(), "grace@example.com".to_string(), "123-456-7896".to_string(), "129 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62707".to_string()],
    ///     vec!["8".to_string(), "Hank".to_string(), "31".to_string(), "hank@example.com".to_string(), "123-456-7897".to_string(), "130 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62708".to_string()],
    ///     vec!["9".to_string(), "Ivy".to_string(), "26".to_string(), "ivy@example.com".to_string(), "123-456-7898".to_string(), "131 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62709".to_string()],
    ///     vec!["10".to_string(), "Jack".to_string(), "29".to_string(), "jack@example.com".to_string(), "123-456-7899".to_string(), "132 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62710".to_string()],
    ///     vec!["11".to_string(), "Karen".to_string(), "34".to_string(), "karen@example.com".to_string(), "123-456-7900".to_string(), "133 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62711".to_string()],
    ///     vec!["12".to_string(), "Leo".to_string(), "21".to_string(), "leo@example.com".to_string(), "123-456-7901".to_string(), "134 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62712".to_string()],
    ///     vec!["13".to_string(), "Mona".to_string(), "32".to_string(), "mona@example.com".to_string(), "123-456-7902".to_string(), "135 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62713".to_string()],
    ///     vec!["14".to_string(), "Nina".to_string(), "23".to_string(), "nina@example.com".to_string(), "123-456-7903".to_string(), "136 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62714".to_string()],
    ///     vec!["15".to_string(), "Oscar".to_string(), "36".to_string(), "oscar@example.com".to_string(), "123-456-7904".to_string(), "137 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62715".to_string()],
    ///     vec!["16".to_string(), "Paul".to_string(), "24".to_string(), "paul@example.com".to_string(), "123-456-7905".to_string(), "138 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62716".to_string()],
    ///     vec!["17".to_string(), "Quinn".to_string(), "37".to_string(), "quinn@example.com".to_string(), "123-456-7906".to_string(), "139 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62717".to_string()],
    ///     vec!["18".to_string(), "Rita".to_string(), "25".to_string(), "rita@example.com".to_string(), "123-456-7907".to_string(), "140 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62718".to_string()],
    ///     vec!["19".to_string(), "Sam".to_string(), "28".to_string(), "sam@example.com".to_string(), "123-456-7908".to_string(), "141 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62719".to_string()],
    ///     vec!["20".to_string(), "Tina".to_string(), "29".to_string(), "tina@example.com".to_string(), "123-456-7909".to_string(), "142 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62720".to_string()],
    ///     vec!["21".to_string(), "Uma".to_string(), "26".to_string(), "uma@example.com".to_string(), "123-456-7910".to_string(), "143 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62721".to_string()],
    ///     vec!["22".to_string(), "Vince".to_string(), "30".to_string(), "vince@example.com".to_string(), "123-456-7911".to_string(), "144 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62722".to_string()],
    ///     vec!["23".to_string(), "Wendy".to_string(), "33".to_string(), "wendy@example.com".to_string(), "123-456-7912".to_string(), "145 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62723".to_string()],
    ///     vec!["24".to_string(), "Xander".to_string(), "34".to_string(), "xander@example.com".to_string(), "123-456-7913".to_string(), "146 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62724".to_string()],
    ///     vec!["25".to_string(), "Yara".to_string(), "31".to_string(), "yara@example.com".to_string(), "123-456-7914".to_string(), "147 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62725".to_string()],
    ///     vec!["26".to_string(), "Zane".to_string(), "27".to_string(), "zane@example.com".to_string(), "123-456-7915".to_string(), "148 Maple St".to_string(), "Springfield".to_string(), "IL".to_string(), "USA".to_string(), "62726".to_string()],
    /// ];
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.heavy_print_table().await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn heavy_print_table(&mut self) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.clone();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let _ = writeln!(temp_file, "{}", temp_builder.headers.join(","));
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = String::from_utf8(row.clone()).unwrap_or_default();
                if row_str.split(',').count() == header_len {
                    let escaped_row = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {:?}", row_str);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, "GET_SUMMARY").await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        // println!("Result:\n{}", pretty_res);

                        // Parse the JSON response
                        if let Ok(parsed_res) =
                            serde_json::from_str::<serde_json::Value>(&pretty_res)
                        {
                            if let Some(rows) = parsed_res.get("rows").and_then(|v| v.as_object()) {
                                // Headers
                                let headers = self.headers.clone();
                                let show_rows = 5;
                                let max_cell_width: usize = 45;
                                let max_columns = 8; // Total columns including the <<+X cols>> column and the last 3 columns

                                // Collect and sort the row keys
                                let mut sorted_keys: Vec<usize> = rows
                                    .keys()
                                    .filter_map(|k| k.parse::<usize>().ok())
                                    .collect();
                                sorted_keys.sort();

                                let total_rows = *sorted_keys.last().unwrap_or(&0);
                                let displayed_rows = sorted_keys.len();
                                let omitted_row_count = total_rows - displayed_rows;

                                // Calculate the maximum length for each column based on visible rows
                                let mut max_lengths =
                                    headers.iter().map(|h| h.len() + 1).collect::<Vec<usize>>();
                                for &key in sorted_keys.iter().take(show_rows).chain(
                                    sorted_keys
                                        .iter()
                                        .skip(displayed_rows.saturating_sub(show_rows)),
                                ) {
                                    if let Some(row) =
                                        rows.get(&key.to_string()).and_then(|v| v.as_object())
                                    {
                                        for (i, header) in headers.iter().enumerate() {
                                            if let Some(cell) =
                                                row.get(header).and_then(|v| v.as_str())
                                            {
                                                let current_max =
                                                    std::cmp::max(max_lengths[i], cell.len());
                                                max_lengths[i] =
                                                    std::cmp::min(current_max, max_cell_width);
                                            }
                                        }
                                    }
                                }

                                // Detect omitted columns
                                let total_columns = headers.len();
                                let omitted_column_count = if total_columns > max_columns {
                                    total_columns - (max_columns - 1) // Last 3 columns plus the <<+X cols>> column should be shown
                                } else {
                                    0
                                };

                                // Truncate headers and max lengths for display
                                let headers_to_display = if total_columns > max_columns {
                                    headers
                                        .iter()
                                        .take(max_columns - 4)
                                        .cloned()
                                        .chain(std::iter::once(format!(
                                            "  <<+{} cols>> ",
                                            omitted_column_count
                                        )))
                                        .chain(headers.iter().skip(total_columns - 3).cloned())
                                        .collect::<Vec<_>>()
                                } else {
                                    headers.clone()
                                };

                                let max_lengths_to_display = if total_columns > max_columns {
                                    max_lengths
                                        .iter()
                                        .take(max_columns - 4)
                                        .cloned()
                                        .chain(std::iter::once(15)) // Ensure <<+X cols>> is aligned
                                        .chain(max_lengths.iter().skip(total_columns - 3).cloned())
                                        .collect::<Vec<_>>()
                                } else {
                                    max_lengths.clone()
                                };

                                // Function to truncate and pad string based on column max length
                                let format_cell = |s: &str, max_length: usize| -> String {
                                    format!("{:width$}", s, width = max_length)
                                };

                                // Function to create the row for omitted columns placeholder
                                let format_omitted_cell = |max_length: usize| -> String {
                                    format!("{:width$}", "...", width = max_length)
                                };

                                // Print the headers
                                let header_line = headers_to_display
                                    .iter()
                                    .zip(max_lengths_to_display.iter())
                                    .map(|(header, &max_length)| format_cell(header, max_length))
                                    .collect::<Vec<String>>()
                                    .join("|");
                                println!("\n|{}|", header_line);
                                println!("{}", "-".repeat(header_line.len() + 2));

                                // Print function for rows
                                let print_row =
                                    |row: &serde_json::Map<String, serde_json::Value>, _max_lengths: &Vec<usize>| {
                                        let row_line = headers_to_display
                                            .iter()
                                            .zip(max_lengths_to_display.iter())
                                            .map(|(header, &max_length)| {
                                                if header.starts_with("  <<+") {
                                                    format_omitted_cell(max_length)
                                                } else {
                                                    row.get(header)
                                                        .and_then(|v| v.as_str())
                                                        .map(|cell| format_cell(cell, max_length))
                                                        .unwrap_or_else(|| {
                                                            format_cell("...", max_length)
                                                        })
                                                }
                                            })
                                            .collect::<Vec<String>>()
                                            .join("|");
                                        println!("|{}|", row_line);
                                    };

                                // Print the first `show_rows`
                                for &key in sorted_keys.iter().take(show_rows) {
                                    if let Some(row) =
                                        rows.get(&key.to_string()).and_then(|v| v.as_object())
                                    {
                                        print_row(row, &max_lengths_to_display);
                                    }
                                }

                                // Print the ellipsis and omitted row count
                                if total_rows > 2 * show_rows {
                                    let row_word = if omitted_row_count == 1 {
                                        "row"
                                    } else {
                                        "rows"
                                    };
                                    println!("<<+{} {}>>", omitted_row_count, row_word);

                                    // Print the last `show_rows`
                                    for &key in sorted_keys.iter().rev().take(show_rows).rev() {
                                        if let Some(row) =
                                            rows.get(&key.to_string()).and_then(|v| v.as_object())
                                        {
                                            print_row(row, &max_lengths_to_display);
                                        }
                                    }
                                } else if displayed_rows > show_rows {
                                    for &key in sorted_keys
                                        .iter()
                                        .skip(show_rows)
                                        .take(displayed_rows - show_rows)
                                    {
                                        if let Some(row) =
                                            rows.get(&key.to_string()).and_then(|v| v.as_object())
                                        {
                                            print_row(row, &max_lengths_to_display);
                                        }
                                    }
                                }

                                // Print omitted columns
                                if omitted_column_count > 0 {
                                    let omitted_columns: Vec<String> =
                                        headers.iter().skip(max_columns - 4).cloned().collect();
                                    println!("\nOmitted columns: {}", omitted_columns.join(", "));
                                }

                                // Print total number of rows
                                println!("Total rows: {}", total_rows);
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Returns a range of rows formatted as JSON-like strings with headers.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.heavy_print_rows_range("2", "3").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```
    pub async fn heavy_print_rows_range(&mut self, start: &str, end: &str) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        //let start_index: usize = start.parse().unwrap_or(0);
        //let end_index: usize = end.parse().unwrap_or(start_index + 1);

        let temp_builder = self.clone();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {}", row_str);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            if let Some(csv_path_str) = temp_file.path().to_str() {
                dbg!(&start, &end);
                let range_string = format!("GET_ROW_RANGE:{}_{}", start, end);
                match DaskConnect::dask_get_inspection_stats(csv_path_str, &range_string).await {
                    Ok(res) => {
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Returns a range of rows formatted as JSON-like strings with headers.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.heavy_print_first_row().await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn heavy_print_first_row(&mut self) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.clone();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {}", row_str);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, "GET_FIRST_ROW").await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Returns a range of rows formatted as JSON-like strings with headers.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.heavy_print_last_row().await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn heavy_print_last_row(&mut self) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.clone();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {}", row_str);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, "GET_LAST_ROW").await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Returns a range of rows formatted as JSON-like strings with headers.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.heavy_print_first_n_rows("2").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn heavy_print_first_n_rows(&mut self, n: &str) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.clone();
        let n_string = format!("GET_FIRST_N_ROWS:{}", n);

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {}", row_str);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, &n_string).await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Returns a range of rows formatted as JSON-like strings with headers.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
    ///     vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    /// ];
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_from_raw_data(headers.clone(), data.clone());
    ///
    /// let result_str = builder.heavy_print_last_n_rows("2").await;
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// });
    ///
    /// ```

    pub async fn heavy_print_last_n_rows(&mut self, n: &str) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.clone();
        let n_string = format!("GET_LAST_N_ROWS:{}", n);

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {}", row_str);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_inspection_stats(csv_path_str, &n_string).await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Groups data by a specified column and transforms the grouped data into a new column containing serialized JSON strings, such that the result is sorted as per the specified column in ascending order, and the elements of grouped data are arranged in a consistent order of key value pairs as per the original builder object.
    ///
    /// This method restructures the CSV data by grouping rows based on the values of a specified column
    /// and then serializing the grouped rows into JSON format. It replaces the current headers with the
    /// group column, a new column for the transformed data, and additional columns based on specified feature flags.
    /// This is particularly useful for aggregation or detailed analysis of subsets within the data.
    ///
    /// # Arguments
    ///
    /// * `group_by` - A string slice that specifies the header name of the column to group the data by.
    /// * `new_column_name` - A string slice that specifies the name of the new column where the grouped
    ///                       and transformed data will be stored.
    /// * `feature_flags` - A vector of tuples where each tuple contains a column name and a feature flag
    ///                     that specifies the type of aggregation or calculation to be applied to the column.
    ///
    /// # Feature Flags
    ///
    /// The following feature flags can be used to perform different types of calculations on the specified columns:
    /// - `COUNT_UNIQUE` - Counts the unique values in the column.
    /// - `NUMERICAL_MAX` - Finds the maximum numerical value in the column.
    /// - `NUMERICAL_MIN` - Finds the minimum numerical value in the column.
    /// - `NUMERICAL_SUM` - Calculates the sum of numerical values in the column.
    /// - `NUMERICAL_MEAN` - Calculates the mean (average) of numerical values in the column, rounded to two decimal places.
    /// - `NUMERICAL_MEDIAN` - Calculates the median of numerical values in the column, rounded to two decimal places.
    /// - `NUMERICAL_STANDARD_DEVIATION` - Calculates the standard deviation of numerical values in the column, rounded to two decimal places.
    /// - `DATETIME_MAX` - Finds the maximum datetime value in the column, based on specified formats.
    /// - `DATETIME_MIN` - Finds the minimum datetime value in the column, based on specified formats.
    /// - `DATETIME_COMMA_SEPARATED` - Comma separates datetime values in the columns
    /// - `MODE` - Finds the most frequent value in the column.
    /// - `BOOL_PERCENT` - Calculates the percentage of `1`s in the column, assuming the values are either `1` or `0`, rounded to two decimal places.
    ///
    /// # Panics
    ///
    /// - Panics if the specified `group_by` column does not exist in the headers.
    /// - Panics if any row does not contain enough columns as expected based on the headers, indicating
    ///   an index out of bounds.
    ///
    ///
    /// # Example 1: Small File Functionality Test
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskGrouperConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "flag".to_string()];
    /// let data = vec![
    ///     vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "1".to_string()],
    ///     vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "0".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "1".to_string()],
    ///     vec!["1".to_string(), "Charlie".to_string(), "28".to_string(), "2023-01-01 11:00:00".to_string(), "0".to_string()]
    /// ];
    ///
    /// let mut csv_builder = HeavyCsvBuilder::heavy_from_raw_data(headers, data);
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let dask_grouper_config = DaskGrouperConfig {
    ///     group_by_column_name: "category".to_string(),
    ///     count_unique_agg_columns: "name".to_string(),
    ///     numerical_max_agg_columns: "age".to_string(),
    ///     numerical_min_agg_columns: "age".to_string(),
    ///     numerical_sum_agg_columns: "age".to_string(),
    ///     numerical_mean_agg_columns: "age".to_string(),
    ///     numerical_median_agg_columns: "age".to_string(),
    ///     numerical_std_deviation_agg_columns: "age".to_string(),
    ///     mode_agg_columns: "name".to_string(),
    ///     datetime_max_agg_columns: "date".to_string(),
    ///     datetime_min_agg_columns: "date".to_string(),
    ///     datetime_semi_colon_separated_agg_columns: "date".to_string(),
    ///     bool_percent_agg_columns: "flag".to_string(),
    /// };
    ///
    /// csv_builder.heavy_grouped_index_transform(
    ///     dask_grouper_config
    /// ).await;
    /// //dbg!(&csv_builder);
    /// //dbg!("AAA");
    /// csv_builder.heavy_print_table().await;
    ///
    ///
    ///
    /// /// Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec![
    ///     "category".to_string(),  
    ///     "COUNT_TOTAL".to_string(),
    ///     "COUNT_UNIQUE".to_string(),
    ///     "age_NUMERICAL_MAX".to_string(),
    ///     "age_NUMERICAL_MEAN".to_string(),
    ///     "age_NUMERICAL_MEDIAN".to_string(),
    ///     "age_NUMERICAL_MIN".to_string(),
    ///     "age_NUMERICAL_STANDARD_DEVIATION".to_string(),
    ///     "age_NUMERICAL_SUM".to_string(),
    ///     "date_DATETIME_MAX".to_string(),
    ///     "date_DATETIME_MIN".to_string(),
    ///     "date_DATETIME_SEMI_COLON_SEPARATED".to_string(),
    ///     "flag_BOOL_PERCENT".to_string(),
    ///     "name_COUNT_UNIQUE".to_string(),
    ///     "name_MODE".to_string()
    ///     ];
    ///
    /// assert_eq!(csv_builder.heavy_get_headers(), &expected_headers, "Headers do not match expected values.");
    ///
    /// let data = csv_builder.heavy_get_data_as_string().unwrap();
    /// //dbg!(&raw_data);
    ///
    /// let expected_data = r#"1,3,3,30.0,27.67,28.0,25.0,2.52,83.0,2023-01-02 13:00:00,2023-01-01 11:00:00,2023-01-01 12:00:00;2023-01-02 13:00:00;2023-01-01 11:00:00,66.67,2,Charlie
    /// 2,1,1,22.0,22.0,22.0,22.0,nan,22.0,2022-12-31 11:59:59,2022-12-31 11:59:59,2022-12-31 11:59:59,0.0,1,Bob
    /// "#;
    ///
    /// assert_eq!(data, expected_data);
    /// //assert_eq!(1,2);
    /// });
    ///
    /// ```

    /*
    ///
    /// # Example 2: Large File Stress Test
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskGrouperConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let mut csv_builder = HeavyCsvBuilder::heavy_from_csv("/home/rgw/Downloads/df_usage.csv").expect("Failed to create HeavyCsvBuilder");
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let dask_grouper_config = DaskGrouperConfig {
    ///     group_by_column_name: "mobile".to_string(),
    ///     count_unique_agg_columns: "".to_string(),
    ///     numerical_max_agg_columns: "".to_string(),
    ///     numerical_min_agg_columns: "".to_string(),
    ///     numerical_sum_agg_columns: "data_used, nmbr_sessions".to_string(),
    ///     numerical_mean_agg_columns: "data_used, nmbr_sessions".to_string(),
    ///     numerical_median_agg_columns: "data_used, nmbr_sessions".to_string(),
    ///     numerical_std_deviation_agg_columns: "".to_string(),
    ///     mode_agg_columns: "".to_string(),
    ///     datetime_max_agg_columns: "".to_string(),
    ///     datetime_min_agg_columns: "".to_string(),
    ///     datetime_semi_colon_separated_agg_columns: "login_dt".to_string(),
    ///     bool_percent_agg_columns: "".to_string(),
    /// };
    ///
    /// csv_builder.heavy_grouped_index_transform(
    ///     dask_grouper_config
    /// ).await;
    /// //dbg!(&csv_builder);
    ///
    /// //dbg!("AAA");
    /// let table = csv_builder.heavy_get_print_table();
    /// println!("{}", table);
    ///
    /// assert_eq!(1,2);
    /// });
    /// ```
     */

    pub async fn heavy_grouped_index_transform(
        &mut self,
        dask_grouper_config: DaskGrouperConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.heavy_from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                let escaped_row: Vec<String>;
                if cells.len() < header_len {
                    // Pad the row with empty strings if it has fewer columns than headers
                    escaped_row = cells
                        .clone()
                        .into_iter()
                        .chain(std::iter::repeat(String::new()).take(header_len - cells.len()))
                        .collect();
                } else if cells.len() > header_len {
                    // Trim the row if it has more columns than headers
                    escaped_row = cells.into_iter().take(header_len).collect();
                } else {
                    // Use the row as is if it has the correct number of columns
                    escaped_row = cells;
                }
                let _ = writeln!(temp_file, "{}", escaped_row.join(","));
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_grouper(csv_path_str, dask_grouper_config).await {
                    Ok((headers, rows)) => {
                        // Convert rows from Vec<Vec<String>> to Vec<Vec<u8>>
                        let rows = rows
                            .iter()
                            .map(|row| row.join(",").into_bytes())
                            .collect::<Vec<Vec<u8>>>();

                        self.headers = headers;
                        self.data = rows;
                    }
                    Err(e) => {
                        println!("{}", &e);
                        // Handle the error, possibly by logging or returning a default value
                        // For now, we just set empty headers and data
                        self.headers = vec![];
                        self.data = vec![];
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskPivoterConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let csv_path = current_dir.join("test_file_samples/pivoting_test_files/employees.csv");
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let mut csv_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_str);
    ///
    /// let dask_pivoter_config = DaskPivoterConfig {
    ///     group_by_column_name: "department".to_string(),
    ///     values_to_aggregate_column_name: "salary".to_string(),
    ///     operation: "NUMERICAL_MEAN".to_string(),
    ///     segregate_by_column_names: "is_manager, gender".to_string(),
    /// };
    ///
    /// csv_builder.heavy_pivot(
    ///     dask_pivoter_config
    /// ).await;
    /// dbg!(&csv_builder);
    /// //dbg!("AAA");
    /// csv_builder.heavy_print_table().await;
    ///
    ///
    ///
    /// /// Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec![
    ///     "department".to_string(),  
    ///     "[is_manager(0)]&&[gender(F)]".to_string(),
    ///     "[is_manager(0)]&&[gender(M)]".to_string(),
    ///     "[is_manager(1)]&&[gender(F)]".to_string(),
    ///     "[is_manager(1)]&&[gender(M)]".to_string(),
    ///     "OVERALL_NUMERICAL_MEAN(salary)".to_string(),
    ///     ];
    ///
    /// assert_eq!(csv_builder.heavy_get_headers(), &expected_headers, "Headers do not match expected values.");
    ///
    /// let data = csv_builder.heavy_get_data_as_string().unwrap();
    /// dbg!(&data);
    ///
    /// let expected_data = r#"Engineering,98000.0,0.0,0.0,100000.0,198000.0
    /// HR,90000.0,0.0,95000.0,75000.0,260000.0
    /// Sales,0.0,52500.0,0.0,105000.0,157500.0
    /// "#;
    ///
    /// assert_eq!(data, expected_data);
    /// //assert_eq!(1,2);
    /// });
    ///
    /// ```

    pub async fn heavy_pivot(&mut self, dask_pivoter_config: DaskPivoterConfig) -> &mut Self {
        //dbg!("BBBB");

        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.heavy_from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {}", row_str);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                //dbg!(csv_path_str);
                match DaskConnect::dask_pivoter(csv_path_str, dask_pivoter_config).await {
                    Ok((headers, rows)) => {
                        //dbg!(&headers);

                        // Convert rows from Vec<Vec<String>> to Vec<Vec<u8>>
                        let rows = rows
                            .iter()
                            .map(|row| row.join(",").into_bytes())
                            .collect::<Vec<Vec<u8>>>();

                        //dbg!(&rows);
                        self.headers = headers;
                        self.data = rows;
                    }
                    Err(e) => {
                        println!("{}", &e);
                        // Handle the error, possibly by logging or returning a default value
                        // For now, we just set empty headers and data
                        self.headers = vec![];
                        self.data = vec![];
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskCleanerConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    /// let csv_path_str = csv_path.to_str().unwrap();
    ///
    /// let mut csv_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_str);
    ///
    /// let dask_cleaner_config = DaskCleanerConfig {
    ///     rules: "mobile:IS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER".to_string(),
    ///     action: "ANALYZE_AND_CLEAN".to_string(),
    ///     show_unclean_values_in_report: "TRUE".to_string(),
    /// };
    ///
    /// csv_builder.heavy_clean_or_test_clean_by_eliminating_rows_subject_to_column_parse_rules(
    ///     dask_cleaner_config
    /// ).await;
    ///
    ///
    /// //csv_builder.print_table();
    /// //dbg!(&csv_builder);
    /// dbg!(&csv_builder);
    /// //dbg!("AAA");
    /// csv_builder.heavy_print_table().await;
    ///
    /// // Use the `get_headers` getter to check headers, handle `Option` returned
    /// let expected_headers = vec![
    ///     "mobile".to_string(),
    ///     "name".to_string(),
    ///     "age".to_string(),
    ///     ];
    /// assert_eq!(csv_builder.heavy_get_headers(), &expected_headers, "Headers do not match expected values.");
    ///
    /// let data = csv_builder.heavy_get_data_as_string().unwrap();
    /// //dbg!(&raw_data);
    ///
    /// let expected_data = r#"9876543210,John Doe,28
    /// 9988776655,Emily Davis,32
    /// 8877665544,Another Invalid,45
    /// "#;
    ///
    /// assert_eq!(data, expected_data);
    /// });
    /// ```
    pub async fn heavy_clean_or_test_clean_by_eliminating_rows_subject_to_column_parse_rules(
        &mut self,
        dask_cleaner_config: DaskCleanerConfig,
    ) -> &mut Self {
        //dbg!("BBBB");

        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.heavy_from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    //println!("Skipping malformed row: {}", row_str);
                    continue;
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                //dbg!(csv_path_str);
                match DaskConnect::dask_cleaner(csv_path_str, dask_cleaner_config.clone()).await {
                    Ok((headers, rows, report)) => {
                        //dbg!(&headers);

                        //dbg!(&rows);

                        if dask_cleaner_config.action == "CLEAN"
                            || dask_cleaner_config.action == "ANALYZE_AND_CLEAN"
                        {
                            self.headers = headers;
                            let rows = rows
                                .iter()
                                .map(|row| row.join(",").into_bytes())
                                .collect::<Vec<Vec<u8>>>();
                            self.data = rows;
                        }

                        // Pretty print the report
                        if dask_cleaner_config.action == "ANALYZE"
                            || dask_cleaner_config.action == "ANALYZE_AND_CLEAN"
                        {
                            match serde_json::to_string_pretty(&report) {
                                Ok(pretty_report) => {
                                    println!("Cleanliness Report:\n{}", pretty_report);
                                }
                                Err(e) => {
                                    println!("Failed to pretty print report: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}", &e);
                        // Handle the error, possibly by logging or returning a default value
                        // For now, we just set empty headers and data
                        //self.headers = vec![];
                        //self.data = vec![];
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the frequency of each unique value in specified columns, sorted by numbers or timestamps if possible.
    /// If values cannot be parsed as numbers or timestamps, they are sorted in ascending alphabetical order.
    /// This method is useful for quickly analyzing the distribution of data within specific columns of a dataset.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of strings representing the names of the columns for which frequencies should be printed.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::{DaskFreqLinearConfig, DaskConnect};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///  let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
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
    /// let mut builder = HeavyCsvBuilder::heavy_from_csv(csv_path_str);
    /// builder.heavy_print_table().await.heavy_print_freq(dask_freq_linear_config).await;
    /// assert_eq!(1,1);
    /// //assert_eq!(1,2);
    /// });
    /// ```
    /// // This will output:
    ///
    /// Frequency for column 'event':
    ///   Independence Day: f = 1 (33.33%)
    ///   New Year         : f = 1 (33.33%)
    ///   New Year's Day   : f = 1 (33.33%)
    ///
    ///
    /// Note: This output assumes that the `print_freq` method properly calculates and formats frequencies as demonstrated.

    pub async fn heavy_print_freq(
        &mut self,
        dask_freq_linear_config: DaskFreqLinearConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.heavy_from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();

            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    continue;
                    //println!("Skipping malformed row: {}", row_str);
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_freq_linear_stats(csv_path_str, dask_freq_linear_config)
                    .await
                {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the frequency of each unique value in specified columns, sorted by numbers or timestamps if possible.
    /// If values cannot be parsed as numbers or timestamps, they are sorted in ascending alphabetical order.
    /// This method is useful for quickly analyzing the distribution of data within specific columns of a dataset.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of strings representing the names of the columns for which frequencies should be printed.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::{DaskFreqCascadingConfig, DaskConnect};
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///  let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let columns = "mobile, name";
    ///
    ///     let dask_freq_linear_config = DaskFreqCascadingConfig {
    ///         column_names: columns.to_string(),
    ///         order_by: "FREQ_DESC".to_string(),
    ///         limit: "".to_string()
    ///     };
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_from_csv(csv_path_str);
    /// builder.heavy_print_table().await.heavy_print_freq_cascading(dask_freq_linear_config).await;
    /// assert_eq!(1,1);
    /// //assert_eq!(1,2);
    /// });
    /// ```
    /// // This will output:
    ///
    /// Frequency for column 'event':
    ///   Independence Day: f = 1 (33.33%)
    ///   New Year         : f = 1 (33.33%)
    ///   New Year's Day   : f = 1 (33.33%)
    ///
    ///
    /// Note: This output assumes that the `print_freq` method properly calculates and formats frequencies as demonstrated.

    pub async fn heavy_print_freq_cascading(
        &mut self,
        dask_freq_cascading_config: DaskFreqCascadingConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.heavy_from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    continue;
                    //println!("Skipping malformed row: {}", row_str);
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_freq_cascading_stats(
                    csv_path_str,
                    dask_freq_cascading_config,
                )
                .await
                {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// Prints the frequency of each unique value in specified columns, sorted by numbers or timestamps if possible.
    /// If values cannot be parsed as numbers or timestamps, they are sorted in ascending alphabetical order.
    /// This method is useful for quickly analyzing the distribution of data within specific columns of a dataset.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of strings representing the names of the columns for which frequencies should be printed.
    ///
    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskConnect;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    ///  let csv_path = current_dir.join("test_file_samples/cleaning_test_files/people.csv");
    ///
    ///     // Convert the path to a string
    ///     let csv_path_str = csv_path.to_str().unwrap();
    ///     let columns = "mobile, name";
    ///
    ///
    /// let mut builder = HeavyCsvBuilder::heavy_from_csv(csv_path_str);
    /// builder.heavy_print_table().await.heavy_print_unique_values_stats(&columns).await;
    /// assert_eq!(1,1);
    /// //assert_eq!(1,2);
    /// });
    /// ```
    /// // This will output:
    ///
    /// Frequency for column 'event':
    ///   Independence Day: f = 1 (33.33%)
    ///   New Year         : f = 1 (33.33%)
    ///   New Year's Day   : f = 1 (33.33%)
    ///
    ///
    /// Note: This output assumes that the `print_freq` method properly calculates and formats frequencies as demonstrated.

    pub async fn heavy_print_unique_values_stats(&mut self, columns: &str) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder = self.heavy_from_copy();

        if let Ok(mut temp_file) = NamedTempFile::new() {
            if !temp_builder.headers.is_empty() {
                let headers = temp_builder
                    .headers
                    .iter()
                    .map(|h| escape_csv_value(h))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(temp_file, "{}", headers);
                println!("Headers: {}", headers);
            }

            // Write data to the temporary file
            let header_len = temp_builder.headers.len();
            for row in &temp_builder.data {
                let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                let cells: Vec<String> = row_str
                    .split(',')
                    .map(|cell| escape_csv_value(cell))
                    .collect();
                if cells.len() == header_len {
                    let escaped_row = cells.join(",");
                    let _ = writeln!(temp_file, "{}", escaped_row);
                } else {
                    continue;
                    //println!("Skipping malformed row: {}", row_str);
                }
            }

            // Flush to ensure all data is written to the file
            let _ = temp_file.flush();

            // Get the path of the temporary file
            if let Some(csv_path_str) = temp_file.path().to_str() {
                match DaskConnect::dask_get_unique_value_stats(csv_path_str, columns).await {
                    Ok(res) => {
                        println!();
                        let pretty_res = serde_json::to_string_pretty(&res).unwrap();
                        println!("Result:\n{}", pretty_res);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            } else {
                dbg!("Failed to convert temp file path to string");
            }
        } else {
            dbg!("Failed to create named temporary file");
        }

        self
    }

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskJoinerConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_a_str);
    ///
    ///
    /// let dask_joiner_config = DaskJoinerConfig {
    ///     join_type: "LEFT_JOIN".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.heavy_union_with_csv_file_big(&csv_path_b_str, dask_joiner_config).await;
    ///
    ///
    /// result.heavy_print_table().await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn heavy_union_with_csv_file_big(
        &mut self,
        file_b_path: &str,
        dask_joiner_config: DaskJoinerConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.heavy_from_copy();
        let temp_builder_b = HeavyCsvBuilder::heavy_from_csv(file_b_path);

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let headers = temp_builder_a
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_a, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_a {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !temp_builder_b.headers.is_empty() {
                    let headers = temp_builder_b
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_b, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_b = temp_builder_b.headers.len();
                for row in &temp_builder_b.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_b {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_joiner(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_joiner_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                let rows = rows
                                    .iter()
                                    .map(|row| row.join(",").into_bytes())
                                    .collect::<Vec<Vec<u8>>>();

                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskJoinerConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_a_str);
    /// let csv_b_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_b_str);
    ///
    ///
    /// let dask_joiner_config = DaskJoinerConfig {
    ///     join_type: "LEFT_JOIN".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.heavy_union_with_csv_builder_big(csv_b_builder, dask_joiner_config).await;
    ///
    ///
    /// result.heavy_print_table().await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn heavy_union_with_csv_builder_big(
        &mut self,
        file_b_builder: HeavyCsvBuilder,
        dask_joiner_config: DaskJoinerConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.heavy_from_copy();

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let headers = temp_builder_a
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_a, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_a {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !file_b_builder.headers.is_empty() {
                    let headers = file_b_builder
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_b, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_b = file_b_builder.headers.len();
                for row in &file_b_builder.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_b {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_joiner(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_joiner_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                let rows = rows
                                    .iter()
                                    .map(|row| row.join(",").into_bytes())
                                    .collect::<Vec<Vec<u8>>>();

                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskIntersectorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_a_str);
    ///
    ///
    /// let dask_intersector_config = DaskIntersectorConfig {
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.heavy_intersection_with_csv_file_big(&csv_path_b_str, dask_intersector_config).await;
    ///
    ///
    /// result.heavy_print_table().await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn heavy_intersection_with_csv_file_big(
        &mut self,
        file_b_path: &str,
        dask_intersector_config: DaskIntersectorConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.heavy_from_copy();
        let temp_builder_b = HeavyCsvBuilder::heavy_from_csv(file_b_path);

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let headers = temp_builder_a
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_a, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_a {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !temp_builder_b.headers.is_empty() {
                    let headers = temp_builder_b
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_b, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_b = temp_builder_b.headers.len();
                for row in &temp_builder_b.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_b {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_intersector(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_intersector_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                let rows = rows
                                    .iter()
                                    .map(|row| row.join(",").into_bytes())
                                    .collect::<Vec<Vec<u8>>>();

                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskIntersectorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/intersection_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/intersection_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_a_str);
    /// let csv_b_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_b_str);
    ///
    ///
    /// let dask_intersector_config = DaskIntersectorConfig {
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.heavy_intersection_with_csv_builder_big(csv_b_builder, dask_intersector_config).await;
    ///
    ///
    /// result.heavy_print_table().await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn heavy_intersection_with_csv_builder_big(
        &mut self,
        file_b_builder: HeavyCsvBuilder,
        dask_intersector_config: DaskIntersectorConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.heavy_from_copy();

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let headers = temp_builder_a
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_a, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_a {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !file_b_builder.headers.is_empty() {
                    let headers = file_b_builder
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_b, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_b = file_b_builder.headers.len();
                for row in &file_b_builder.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_b {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_intersector(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_intersector_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                let rows = rows
                                    .iter()
                                    .map(|row| row.join(",").into_bytes())
                                    .collect::<Vec<Vec<u8>>>();

                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskDifferentiatorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_a_str);
    ///
    ///
    /// let dask_differentiator_config = DaskDifferentiatorConfig {
    ///     difference_type: "NORMAL".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.heavy_difference_with_csv_file_big(&csv_path_b_str, dask_differentiator_config).await;
    ///
    ///
    /// result.heavy_print_table().await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn heavy_difference_with_csv_file_big(
        &mut self,
        file_b_path: &str,
        dask_differentiator_config: DaskDifferentiatorConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.heavy_from_copy();
        let temp_builder_b = HeavyCsvBuilder::heavy_from_csv(file_b_path);

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let headers = temp_builder_a
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_a, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_a {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !temp_builder_b.headers.is_empty() {
                    let headers = temp_builder_b
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_b, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_b = temp_builder_b.headers.len();
                for row in &temp_builder_b.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_b {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_differentiator(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_differentiator_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                let rows = rows
                                    .iter()
                                    .map(|row| row.join(",").into_bytes())
                                    .collect::<Vec<Vec<u8>>>();

                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// ```
    /// use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    /// use rgwml::dask_utils::DaskDifferentiatorConfig;
    /// use std::collections::HashMap;
    /// use tokio::runtime::Runtime;
    /// use std::path::PathBuf;
    ///
    /// let current_dir = std::env::current_dir().unwrap();
    ///
    /// let csv_path_a = current_dir.join("test_file_samples/joining_test_files/difference_file_a.csv");
    /// let csv_path_a_str = csv_path_a.to_str().unwrap();
    ///
    /// let csv_path_b = current_dir.join("test_file_samples/joining_test_files/difference_file_b.csv");
    /// let csv_path_b_str = csv_path_b.to_str().unwrap();
    ///
    /// let mut csv_a_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_a_str);
    /// let csv_b_builder = HeavyCsvBuilder::heavy_from_csv(&csv_path_b_str);
    ///
    ///
    /// let dask_differentiator_config = DaskDifferentiatorConfig {
    ///     difference_type: "NORMAL".to_string(),
    ///     table_a_ref_column: "id".to_string(),
    ///     table_b_ref_column: "id".to_string(),
    /// };
    ///
    /// let rt = Runtime::new().unwrap();
    /// rt.block_on(async {
    ///
    /// let result = csv_a_builder.heavy_difference_with_csv_builder_big(csv_b_builder, dask_differentiator_config).await;
    ///
    ///
    /// result.heavy_print_table().await;
    /// dbg!(&result);
    ///
    /// });
    /// //assert_eq!(1,2);
    /// assert_eq!(1,1);
    /// ```

    pub async fn heavy_difference_with_csv_builder_big(
        &mut self,
        file_b_builder: HeavyCsvBuilder,
        dask_differentiator_config: DaskDifferentiatorConfig,
    ) -> &mut Self {
        fn escape_csv_value(value: &str) -> String {
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                format!("\"{}\"", value.replace("\"", "\"\""))
            } else {
                value.to_string()
            }
        }

        let temp_builder_a = self.heavy_from_copy();

        if let Ok(mut temp_file_a) = NamedTempFile::new() {
            if let Ok(mut temp_file_b) = NamedTempFile::new() {
                if !temp_builder_a.headers.is_empty() {
                    let headers = temp_builder_a
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_a, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_a = temp_builder_a.headers.len();
                for row in &temp_builder_a.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_a {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_a, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_a.flush();

                if !file_b_builder.headers.is_empty() {
                    let headers = file_b_builder
                        .headers
                        .iter()
                        .map(|h| escape_csv_value(h))
                        .collect::<Vec<_>>()
                        .join(",");
                    let _ = writeln!(temp_file_b, "{}", headers);
                }

                // Write data to the temporary file
                let header_len_b = file_b_builder.headers.len();
                for row in &file_b_builder.data {
                    let row_str = row.iter().map(|byte| char::from(*byte)).collect::<String>();
                    let cells: Vec<String> = row_str
                        .split(',')
                        .map(|cell| escape_csv_value(cell))
                        .collect();
                    if cells.len() == header_len_b {
                        let escaped_row = cells.join(",");
                        let _ = writeln!(temp_file_b, "{}", escaped_row);
                    } else {
                        continue;
                        //println!("Skipping malformed row: {}", row_str);
                    }
                }

                // Flush to ensure all data is written to the file
                let _ = temp_file_b.flush();

                // Get the path of the temporary file
                if let Some(csv_path_a_str) = temp_file_a.path().to_str() {
                    if let Some(csv_path_b_str) = temp_file_b.path().to_str() {
                        match DaskConnect::dask_differentiator(
                            csv_path_a_str,
                            csv_path_b_str,
                            dask_differentiator_config,
                        )
                        .await
                        {
                            Ok((headers, rows)) => {
                                let rows = rows
                                    .iter()
                                    .map(|row| row.join(",").into_bytes())
                                    .collect::<Vec<Vec<u8>>>();

                                self.headers = headers;
                                self.data = rows;
                            }
                            Err(e) => {
                                println!("{}", &e);
                                // Handle the error, possibly by logging or returning a default value
                                // For now, we just set empty headers and data
                                self.headers = vec![];
                                self.data = vec![];
                            }
                        }
                    } else {
                        dbg!("Failed to convert temp file B path to string");
                    }
                } else {
                    dbg!("Failed to convert temp file A path to string");
                }
            } else {
                dbg!("Failed to create named temporary file B");
            }
        } else {
            dbg!("Failed to create named temporary file A");
        }

        self
    }

    /// Terminates the application. This method calls `std::process::exit(0)`, which will stop the program immediately. As such, it's typically not used in regular application logic outside of error handling or specific shutdown conditions.
    #[allow(unreachable_code)]
    pub fn die(&mut self) -> &mut Self {
        println!("Giving up the ghost!");
        std::process::exit(0);
        self
    }
}
