// db_utils.rs
use futures::StreamExt;
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
                    | ColumnType::NText => row
                        .try_get::<&str, _>(i)
                        //            .map_or_else(|_| "".to_string(), |v| v.to_string()),
                        .map_or_else(
                            |_| "".to_string(),
                            |v| match v {
                                Some(value) => value.to_string(),
                                None => "".to_string(),
                            },
                        ),

                    ColumnType::Datetime
                    | ColumnType::Datetime2
                    | ColumnType::Datetime4
                    | ColumnType::Daten
                    | ColumnType::Timen
                    | ColumnType::DatetimeOffsetn => row.try_get::<&str, _>(i).map_or_else(
                        |_| "".to_string(),
                        |v| match v {
                            Some(date_str) => {
                                chrono::NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S")
                                    .map_or_else(|_| "".to_string(), |dt| dt.to_string())
                            }
                            None => "".to_string(),
                        },
                    ),

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





}
