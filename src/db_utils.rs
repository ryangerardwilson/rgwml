// db_utils.rs
use crate::csv_utils::CsvBuilder;
use crate::python_utils::DB_CONNECT_SCRIPT;
use chrono::{NaiveDate, NaiveDateTime, Utc};
use dirs;
use futures::StreamExt;
use mysql_async::{prelude::*, OptsBuilder, Pool, Row as MySqlRow};
use serde_json::Value;
use std::fs::{create_dir_all, read_dir, remove_file, File};
use std::io::{BufWriter, Write};
use std::process::Command;
use tiberius::{error::Error, AuthMethod, Client, ColumnType, Config, QueryItem, Row};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
//use tokio::task;
use lazy_static::lazy_static;
use memmap::MmapOptions;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use uuid::Uuid;

const LIBRARY_VERSION: &str = "1.2.98";

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
    let versioned_python_file_name = format!("db_connect_v{}.py", version.replace('.', "_"));
    let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

    // Remove old version files that do not match the current version
    for entry in read_dir(&exec_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        if file_name_str.starts_with("db_connect_v") && file_name_str != versioned_python_file_name
        {
            println!("Removing old file: {}", file_name_str); // Debugging information
            remove_file(entry.path())?;
        }
    }

    // Check if the versioned Python file already exists
    if !versioned_python_file_path.exists() {
        // Write the Python script to the versioned file
        let mut out = BufWriter::new(File::create(&versioned_python_file_path)?);
        out.write_all(DB_CONNECT_SCRIPT.as_bytes())?;
        out.flush()?;
    }

    Ok(())
}

/// Represents a database connection manager for handling database operations
pub struct DbConnect;

/// Implementation block for DbConnect, providing methods for database interactions
impl DbConnect {
    /// Executes an SQL query against a Microsoft SQL Server database and returns the results or an error
    pub async fn execute_mssql_query(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        async fn create_mssql_connection(
            username: &str,
            password: &str,
            server: &str,
            database: &str,
        ) -> Result<Client<Compat<TcpStream>>, Box<dyn std::error::Error>> {
            let mut config = Config::new();
            config.host(server);
            config.database(database);
            config.port(1433);
            config.authentication(AuthMethod::sql_server(&username, &password));
            config.trust_cert();

            let tcp = TcpStream::connect(config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            let compat_tcp = tcp.compat_write();

            let client = match Client::connect(config, compat_tcp).await {
                Ok(client) => client,
                Err(Error::Routing { host, port }) => {
                    let mut config = Config::new();
                    config.host(&host);
                    config.port(port);
                    config.authentication(AuthMethod::sql_server(username, password));

                    let tcp = TcpStream::connect(config.get_addr()).await?;
                    tcp.set_nodelay(true)?;

                    let compat_tcp = tcp.compat_write();

                    Client::connect(config, compat_tcp).await?
                }
                Err(e) => return Err(e.into()),
            };

            Ok(client)
        }

        fn extract_column_names(row: &Row) -> Vec<String> {
            row.columns()
                .iter()
                .map(|col| col.name().to_string())
                .collect()
        }

        fn extract_row_values(row: &Row) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let mut values = Vec::new();

            //dbg!(&row);

            for i in 0..row.columns().len() {
                let column_type = row.columns()[i].column_type();
                //dbg!(&column_type);
                let value = match column_type {
                    ColumnType::Int1 => row.try_get::<u8, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Int2 => row.try_get::<i16, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Int4 => row.try_get::<i32, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Int8 => row.try_get::<i64, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Intn => {
                        // Try different integer types
                        if let Ok(Some(value)) = row.try_get::<i64, _>(i) {
                            value.to_string()
                        } else if let Ok(Some(value)) = row.try_get::<i32, _>(i) {
                            value.to_string()
                        } else if let Ok(Some(value)) = row.try_get::<i16, _>(i) {
                            value.to_string()
                        } else if let Ok(Some(value)) = row.try_get::<u8, _>(i) {
                            value.to_string()
                        } else {
                            "".to_string()
                        }
                    }
                    ColumnType::Float4 => row.try_get::<f32, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Float8 => row.try_get::<f64, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Bit => row.try_get::<bool, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    // String types
                    ColumnType::BigVarChar
                    | ColumnType::BigChar
                    | ColumnType::NVarchar
                    | ColumnType::NChar
                    | ColumnType::Text
                    | ColumnType::NText => row.try_get::<&str, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Datetime
                    | ColumnType::Datetime2
                    | ColumnType::Datetimen
                    | ColumnType::Daten
                    | ColumnType::Timen
                    | ColumnType::DatetimeOffsetn => {
                        match row.try_get::<NaiveDateTime, _>(i) {
                            Ok(Some(naive_datetime)) => {
                                naive_datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                            } // Format Datetime appropriately
                            Ok(None) => "".to_string(),
                            Err(_) => match row.try_get::<NaiveDate, _>(i) {
                                // Try parsing as NaiveDate if NaiveDateTime fails
                                Ok(Some(naive_date)) => naive_date.format("%Y-%m-%d").to_string(), // Format Date appropriately
                                Ok(None) => "".to_string(),
                                Err(_) => "Error or non-Datetime/Date value".to_string(),
                            },
                        }
                    }

                    ColumnType::Money | ColumnType::Money4 => row.try_get::<f64, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(value) => value.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Guid => row.try_get::<Uuid, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(uuid) => uuid.to_string(),
                            None => "".to_string(),
                        },
                    ),

                    ColumnType::Xml => row
                        .try_get::<&str, _>(i)
                        //                    .map_or_else(|_| "".to_string(), |v| v.to_string()),
                        .map_or_else(
                            |_| "".to_string(),
                            |v| match v {
                                Some(value) => value.to_string(),
                                None => "".to_string(),
                            },
                        ),

                    // ... handle other types similarly
                    _ => "".to_string(),
                };

                values.push(value);
            }

            Ok(values)
        }

        let mut client = create_mssql_connection(username, password, server, database).await?;

        let mut stream = client.simple_query(sql_query).await?;

        let mut headers: Vec<String> = Vec::new();
        let mut data: Vec<Vec<String>> = Vec::new();

        while let Some(query_item_result) = stream.next().await {
            //dbg!(&query_item_result);
            match query_item_result {
                Ok(QueryItem::Row(row)) => {
                    if headers.is_empty() {
                        headers = extract_column_names(&row);
                    }
                    match extract_row_values(&row) {
                        Ok(row_data) => data.push(row_data),
                        Err(e) => return Err(e.into()), // Convert the error to Box<dyn std::error::Error>
                    }
                }
                Ok(_) => continue,              // Skip non-row items
                Err(e) => return Err(e.into()), // Convert the error to Box<dyn std::error::Error>
            }
        }

        //dbg!(&headers, &data);

        Ok((headers, data))
    }

    /// Lists all databases on the server and prints all tables within each database using CsvBuilder for MSSQL queries
    pub async fn print_mssql_databases(
        username: &str,
        password: &str,
        server: &str,
        default_database: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let db_query = "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')";
        let mut databases_result =
            CsvBuilder::from_mssql_query(username, password, server, default_database, db_query)
                .await?;
        databases_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists schemas from the specified MSSQL database
    pub async fn print_mssql_schemas(
        username: &str,
        password: &str,
        server: &str,
        in_focus_database: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema_query = format!(
            "SELECT SCHEMA_NAME FROM {}.INFORMATION_SCHEMA.SCHEMATA",
            in_focus_database
        );

        let mut schemas_result = CsvBuilder::from_mssql_query(
            username,
            password,
            server,
            in_focus_database,
            &schema_query,
        )
        .await?;
        schemas_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists tables from the specified schema within an MSSQL database
    pub async fn print_mssql_tables(
        username: &str,
        password: &str,
        server: &str,
        in_focus_database: &str,
        schema: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Use "dbo" if schema is an empty string
        let effective_schema = if schema.is_empty() { "dbo" } else { schema };

        let table_query = format!(
        "SELECT TABLE_NAME FROM {}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_TYPE = 'BASE TABLE'",
        in_focus_database, effective_schema
    );

        let mut tables_result = CsvBuilder::from_mssql_query(
            username,
            password,
            server,
            in_focus_database,
            &table_query,
        )
        .await?;
        tables_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists column descriptions from the specified table within an MSSQL database
    pub async fn print_mssql_table_description(
        username: &str,
        password: &str,
        server: &str,
        in_focus_database: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // SQL query to fetch column details similar to 'sp_help'
        let column_query = format!(
            "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLLATION_NAME 
        FROM {}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{}'",
            in_focus_database, table_name
        );

        //dbg!(&column_query);
        let mut columns_result = CsvBuilder::from_mssql_query(
            username,
            password,
            server,
            in_focus_database,
            &column_query,
        )
        .await?;
        columns_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists databases along with their schemas and tables architecture in MSSQL
    pub async fn print_mssql_architecture(
        username: &str,
        password: &str,
        server: &str,
        default_database: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async fn list_databases(
            username: &str,
            password: &str,
            server: &str,
            default_database: &str,
        ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let db_query = "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')";
            let databases_result = CsvBuilder::from_mssql_query(
                username,
                password,
                server,
                default_database,
                db_query,
            )
            .await?;

            if let Some(data) = databases_result.get_data() {
                let blank = "".to_string();
                let databases: Vec<String> = data
                    .iter()
                    .map(|row| row.get(0).unwrap_or(&blank).clone()) // Extract owned String directly
                    .collect();
                Ok(databases)
            } else {
                Err("No data returned from query".into())
            }
        }

        // List databases
        Self::print_mssql_databases(username, password, server, default_database).await?;

        // List schemas and tables for each database
        let databases = list_databases(username, password, server, default_database).await?;
        println!();
        for database in databases {
            //dbg!(&database);

            println!("+{:=<width$}+", "", width = database.len() + 2);
            println!("| {} |", database);
            println!("+{:=<width$}+", "", width = database.len() + 2);

            // Handle errors for listing schemas
            if let Err(err) = Self::print_mssql_schemas(username, password, server, &database).await
            {
                eprintln!("\nError listing schemas for {}: {}", database, err);
            }

            // Handle errors for listing tables
            if let Err(err) =
                Self::print_mssql_tables(username, password, server, &database, "dbo").await
            {
                eprintln!("\nError listing tables for {}: {}", database, err);
            }
            println!();
            println!();
        }

        Ok(())
    }

    /// Executes a read-only SQL query against a MySQL database and returns the results or an error
    pub async fn execute_mysql_query(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        // Create an OptsBuilder instance and set the connection details
        let builder = OptsBuilder::default()
            .user(Some(username))
            .pass(Some(password))
            .ip_or_hostname(server)
            .db_name(Some(database));

        // Create a pool with the constructed Opts
        let pool = Pool::new(builder);
        let mut conn = pool.get_conn().await?;

        // Perform the query
        let result: Vec<MySqlRow> = conn.query(sql_query).await?;

        // Process the result
        let mut headers = Vec::new();
        let mut data = Vec::new();

        if let Some(first_row) = result.first() {
            headers = first_row
                .columns_ref()
                .iter()
                .map(|col| col.name_str().to_string())
                .collect::<Vec<String>>();
        }

        for row in result {
            let row_data = (0..headers.len())
                .map(|i| {
                    match row.get_opt::<String, usize>(i) {
                        Some(Ok(value)) => value,
                        _ => String::from("NULL"), // Or any other placeholder you prefer
                    }
                })
                .collect::<Vec<String>>();
            data.push(row_data);
        }

        Ok((headers, data))
    }

    /// Executes a data-writable SQL query against a MySQL database and returns the results or an error
    pub async fn execute_mysql_write(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
        sql_query: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Create an OptsBuilder instance with default settings
        let opts = OptsBuilder::default()
            .user(Some(username.to_string()))
            .pass(Some(password.to_string()))
            .ip_or_hostname(server.to_string()) // Pass server address directly
            .db_name(Some(database.to_string()));

        // Create a pool with the constructed Opts
        let pool = Pool::new(opts);

        // Get a connection from the pool
        let mut conn = pool.get_conn().await?;
        // Execute the query without expecting any result set
        conn.exec_drop(sql_query, ()).await?;

        // Return Ok if the query executed successfully
        Ok(())
    }

    /// Retrieves and lists schemas from the specified MySQL database
    pub async fn print_mysql_databases(
        username: &str,
        password: &str,
        server: &str,
        default_database: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let db_query = "SHOW DATABASES WHERE `Database` NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')";
        //dbg!(&db_query);
        let mut databases_result =
            CsvBuilder::from_mysql_query(username, password, server, default_database, db_query)
                .await?;

        //dbg!(&databases_result);
        databases_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists tables from the specified schema within a MySQL database
    pub async fn print_mysql_tables(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // There's no need to default to "dbo" as MySQL does not use this concept.
        let table_query = format!(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_TYPE = 'BASE TABLE'",
        database
    );

        //dbg!(&table_query);
        let mut tables_result =
            CsvBuilder::from_mysql_query(username, password, server, database, &table_query)
                .await?;
        tables_result.print_table_all_rows();
        //dbg!(&tables_result);

        Ok(())
    }

    /// Retrieves and lists column descriptions from the specified table within a MySQL database
    pub async fn print_mysql_table_description(
        username: &str,
        password: &str,
        server: &str,
        in_focus_database: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // SQL query to fetch column details similar to 'DESCRIBE tablename' or 'SHOW COLUMNS FROM tablename'
        let column_query = format!(
            "SHOW FULL COLUMNS FROM {}.{}",
            in_focus_database, table_name
        );

        //dbg!(&column_query);
        // Use CsvBuilder's from_mysql_query method to execute the query and handle results
        let mut columns_result = CsvBuilder::from_mysql_query(
            username,
            password,
            server,
            in_focus_database,
            &column_query,
        )
        .await?;
        columns_result
            .retain_columns(vec![
                "Field",
                "Type",
                "Null",
                "Default",
                "Extra",
                "Collation",
            ])
            .print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists databases along with their schemas and tables architecture in MySQL
    pub async fn print_mysql_architecture(
        username: &str,
        password: &str,
        server: &str,
        default_database: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async fn list_databases(
            username: &str,
            password: &str,
            server: &str,
            default_database: &str,
        ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let db_query = "SHOW DATABASES WHERE `Database` NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')";
            let databases_result = CsvBuilder::from_mysql_query(
                username,
                password,
                server,
                default_database,
                db_query,
            )
            .await?;

            if let Some(data) = databases_result.get_data() {
                let blank = "".to_string();
                let databases: Vec<String> = data
                    .iter()
                    .map(|row| row.get(0).unwrap_or(&blank).clone()) // Extract owned String directly
                    .collect();
                Ok(databases)
            } else {
                Err("No data returned from query".into())
            }
        }

        // List databases
        Self::print_mysql_databases(&username, &password, &server, &default_database).await?;

        // List schemas and tables for each database
        let databases = list_databases(&username, &password, &server, &default_database).await?;
        //dbg!(&databases);
        println!();
        for database in databases {
            //dbg!(&database);

            println!("+{:=<width$}+", "", width = database.len() + 2);
            println!("| {} |", database);
            println!("+{:=<width$}+", "", width = database.len() + 2);

            // Handle errors for listing tables
            if let Err(err) = Self::print_mysql_tables(username, password, server, &database).await
            {
                eprintln!("\nError listing tables for {}: {}", database, err);
            }
            println!();
            println!();
        }

        Ok(())
    }

    /// Executes an SQL query against a ClickHouse Server database and returns the results or an error
    pub async fn execute_clickhouse_query(
        username: &str,
        password: &str,
        server: &str,
        sql_query: &str,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        /*
        let python_exec_command = task::block_in_place(|| {
            let rt = Runtime::new()?;
            rt.block_on(prepare_executable())
        })?;
        */
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name = format!("db_connect_v{}.py", version.replace('.', "_"));
        let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

        let python_exec_command =
            format!("python3 {}", versioned_python_file_path.to_string_lossy());

        let uid = format!(
            "{}-{}",
            Uuid::new_v4().to_string(),
            Utc::now().timestamp_millis()
        );

        // Execute the Python script with the required arguments
        let args = vec![
            "--uid".to_string(),
            uid.clone(),
            "clickhouse".to_string(),
            server.to_string(),
            "9000".to_string(),
            username.to_string(),
            password.to_string(),
            sql_query.to_string(),
        ];

        // dbg!(&args);

        // Create the full command with arguments
        let full_command = format!(
            "{} {}",
            python_exec_command,
            args.iter()
                .map(|arg| format!("\"{}\"", arg))
                .collect::<Vec<String>>()
                .join(" ")
        );

        // dbg!(&full_command);

        // Execute the command
        let output = Command::new("sh").arg("-c").arg(&full_command).output();

        // Clean up the temporary file
        //remove_file(&temp_path)?;

        // Handle the output from the command
        match output {
            Ok(output) => {
                if !output.status.success() {
                    let stderr = std::str::from_utf8(&output.stderr)?;
                    eprintln!("Script error: {}", stderr);
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        stderr,
                    )));
                }

                //let output_str = std::str::from_utf8(&output.stdout)?;
                //let file = File::open("output.json")?;
                let filename = format!("rgwml_{}.json", uid);
                let file = File::open(&filename)?;

                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

                let json: Value = serde_json::from_str(output_str)?;

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

    /// Retrieves and lists database names from the specified Clickhouse server
    pub async fn print_clickhouse_databases(
        username: &str,
        password: &str,
        server: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let db_query = "SHOW DATABASES";
        //dbg!(&db_query);
        let mut databases_result =
            CsvBuilder::from_clickhouse_query(username, password, server, db_query).await?;

        //dbg!(&databases_result);
        databases_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists tables from the specified Clickhouse server
    pub async fn print_clickhouse_tables(
        username: &str,
        password: &str,
        server: &str,
        database: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // There's no need to default to "dbo" as MySQL does not use this concept.
        let table_query = format!("SHOW TABLES FROM {}", database);

        //dbg!(&table_query);
        let mut tables_result =
            CsvBuilder::from_clickhouse_query(username, password, server, &table_query).await?;
        tables_result.print_table_all_rows();
        //dbg!(&tables_result);

        Ok(())
    }

    /// Retrieves and lists column descriptions from the specified table in the specified
    /// ClickHouse server
    pub async fn print_clickhouse_table_description(
        username: &str,
        password: &str,
        server: &str,
        in_focus_database: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // SQL query to fetch column details similar to 'DESCRIBE tablename' or 'SHOW COLUMNS FROM tablename'
        let column_query = format!(
            "SHOW FULL COLUMNS FROM {}.{}",
            in_focus_database, table_name
        );

        //dbg!(&column_query);
        // Use CsvBuilder's from_mysql_query method to execute the query and handle results
        let mut columns_result =
            CsvBuilder::from_clickhouse_query(username, password, server, &column_query).await?;

        columns_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists databases along with their tables architecture in the specified
    /// ClickHouse server
    pub async fn print_clickhouse_architecture(
        username: &str,
        password: &str,
        server: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async fn list_databases(
            username: &str,
            password: &str,
            server: &str,
        ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let db_query = "SHOW DATABASES";
            let databases_result =
                CsvBuilder::from_clickhouse_query(username, password, server, db_query).await?;

            if let Some(data) = databases_result.get_data() {
                let blank = "".to_string();
                let databases: Vec<String> = data
                    .iter()
                    .map(|row| row.get(0).unwrap_or(&blank).clone()) // Extract owned String directly
                    .collect();
                Ok(databases)
            } else {
                Err("No data returned from query".into())
            }
        }

        // List databases
        Self::print_clickhouse_databases(&username, &password, &server).await?;

        // List schemas and tables for each database
        let databases = list_databases(&username, &password, &server).await?;
        //dbg!(&databases);
        println!();
        for database in databases {
            //dbg!(&database);

            println!("+{:=<width$}+", "", width = database.len() + 2);
            println!("| {} |", database);
            println!("+{:=<width$}+", "", width = database.len() + 2);

            // Handle errors for listing tables
            if let Err(err) =
                Self::print_clickhouse_tables(username, password, server, &database).await
            {
                eprintln!("\nError listing tables for {}: {}", database, err);
            }
            println!();
            println!();
        }

        Ok(())
    }

    /// Executes an SQL query against a GoogleBigQuery Server database and returns the results or an error
    pub async fn execute_google_big_query_query(
        json_credentials_path: &str,
        sql_query: &str,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
        /*
        let python_exec_command = task::block_in_place(|| {
            let rt = Runtime::new()?;
            rt.block_on(prepare_executable())
        })?;
        */
        let version = LIBRARY_VERSION;
        let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
        let rgwml_dir = home_dir.join("RGWML");
        let exec_dir = rgwml_dir.join("executables");
        let versioned_python_file_name = format!("db_connect_v{}.py", version.replace('.', "_"));
        let versioned_python_file_path = exec_dir.join(&versioned_python_file_name);

        let python_exec_command =
            format!("python3 {}", versioned_python_file_path.to_string_lossy());

        // Split the python_exec_command into the interpreter and the script path
        let parts: Vec<&str> = python_exec_command.split_whitespace().collect();
        let python_interpreter = parts[0];
        let script_path = parts[1];

        // Define the arguments
        let args = vec![
            "google_big_query".to_string(),
            json_credentials_path.to_string(),
            sql_query.to_string(),
        ];

        // Create the Command with the interpreter and script path
        let mut command = Command::new(python_interpreter);
        command.arg(script_path);

        for arg in args {
            command.arg(arg);
        }

        //dbg!(&command);

        // Execute the command
        let output = command.output();

        // Handle the output from the command
        match output {
            Ok(output) => {
                if !output.status.success() {
                    let stderr = std::str::from_utf8(&output.stderr)?;
                    eprintln!("Script error: {}", stderr);
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        stderr,
                    )));
                }

                //let output_str = std::str::from_utf8(&output.stdout)?;
                let file = File::open("output.json")?;
                let mmap = unsafe { MmapOptions::new().map(&file)? };

                let output_str = std::str::from_utf8(&mmap)?;

                let json: Value = serde_json::from_str(output_str)?;

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

                Ok((headers, rows))
            }
            Err(e) => {
                eprintln!("Failed to execute command: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// Retrieves and lists database names from the specified Clickhouse server
    pub async fn print_google_big_query_datasets(
        json_credentials_path: &str,
        project_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let db_query = format!(
            "SELECT schema_name FROM `{}.INFORMATION_SCHEMA.SCHEMATA`",
            project_id
        );
        //dbg!(&db_query);
        let mut datasets_result =
            CsvBuilder::from_google_big_query_query(json_credentials_path, &db_query).await?;

        //dbg!(&databases_result);
        datasets_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists tables from the specified Clickhouse server
    pub async fn print_google_big_query_tables(
        json_credentials_path: &str,
        project_id: &str,
        dataset: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // There's no need to default to "dbo" as MySQL does not use this concept.
        let db_query = format!(
            "SELECT table_name FROM `{}.{}.INFORMATION_SCHEMA.TABLES`",
            project_id, dataset
        );

        //dbg!(&table_query);
        let mut tables_result =
            CsvBuilder::from_google_big_query_query(json_credentials_path, &db_query).await?;
        tables_result.print_table_all_rows();
        //dbg!(&tables_result);

        Ok(())
    }

    /// Retrieves and lists column descriptions from the specified table in the specified
    /// ClickHouse server
    pub async fn print_google_big_query_table_description(
        json_credentials_path: &str,
        project_id: &str,
        dataset: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // SQL query to fetch column details similar to 'DESCRIBE tablename' or 'SHOW COLUMNS FROM tablename'
        let column_query = format!(
            "SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{}'`",
            project_id, dataset, table_name
        );

        //dbg!(&column_query);
        // Use CsvBuilder's from_mysql_query method to execute the query and handle results
        let mut columns_result =
            CsvBuilder::from_google_big_query_query(json_credentials_path, &column_query).await?;

        columns_result.print_table_all_rows();

        Ok(())
    }

    /// Retrieves and lists databases along with their tables architecture in the specified
    /// ClickHouse server
    pub async fn print_google_big_query_architecture(
        json_credentials_path: &str,
        project_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async fn list_datasets(
            json_credentials_path: &str,
            project_id: &str,
        ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let db_query = format!(
                "SELECT schema_name FROM `{}.INFORMATION_SCHEMA.SCHEMATA`",
                project_id
            );
            let datasets_result =
                CsvBuilder::from_google_big_query_query(json_credentials_path, &db_query).await?;

            if let Some(data) = datasets_result.get_data() {
                let blank = "".to_string();
                let datasets: Vec<String> = data
                    .iter()
                    .map(|row| row.get(0).unwrap_or(&blank).clone()) // Extract owned String directly
                    .collect();
                Ok(datasets)
            } else {
                Err("No data returned from query".into())
            }
        }

        // List databases
        Self::print_google_big_query_datasets(json_credentials_path, project_id).await?;

        // List schemas and tables for each database
        let datasets = list_datasets(json_credentials_path, project_id).await?;
        //dbg!(&databases);
        println!();
        for dataset in datasets {
            //dbg!(&database);

            println!("+{:=<width$}+", "", width = dataset.len() + 2);
            println!("| {} |", dataset);
            println!("+{:=<width$}+", "", width = dataset.len() + 2);

            // Handle errors for listing tables
            if let Err(err) =
                Self::print_google_big_query_tables(json_credentials_path, project_id, &dataset)
                    .await
            {
                eprintln!("\nError listing tables for {}: {}", dataset, err);
            }
            println!();
            println!();
        }

        Ok(())
    }
}
