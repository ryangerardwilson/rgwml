// db_utils.rs
use chrono::NaiveDateTime;
use futures::StreamExt;
use mysql_async::{prelude::*, OptsBuilder, Pool, Row as MySqlRow};
use tiberius::{error::Error, AuthMethod, Client, ColumnType, Config, QueryItem, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use uuid::Uuid;

pub struct DbConnect;

impl DbConnect {
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

            for i in 0..row.columns().len() {
                let column_type = row.columns()[i].column_type();
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

                    ColumnType::Datetime => {
                        match row.try_get::<NaiveDateTime, _>(i) {
                            Ok(Some(naive_datetime)) => naive_datetime.to_string(), // Format this appropriately
                            Ok(None) => "".to_string(),
                            Err(_) => "Error or non-Datetime value".to_string(),
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
            .ip_or_hostname(Some(server.to_string()))
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

}
