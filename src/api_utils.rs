// api_utils.rs
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Response};
// use serde::Serialize;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fs;
use std::str::FromStr;

/// A flexible builder for sending and caching API requests.
///
/// This struct provides a fluent interface to build API requests with support for method
/// chaining. It simplifies the process by allowing you to specify both headers and payload
/// as `serde_json::Value`. This approach is convenient when dealing with JSON data,
/// making it easy to construct requests dynamically.
///
/// If caching is enabled, responses are stored and reused for subsequent requests made
/// within the specified cache duration.
///
/// Example 1: Without Headers
/// ```
/// use serde_json::json;
/// use rgwml::api_utils::ApiCallBuilder;
///
/// #[tokio::main]
/// async fn main() {
///     let method = "POST"; // Or "GET"
///     let url = "http://example.com/api/submit";
///     let payload = json!({
///         "field1": "Hello",
///         "field2": 123
///     });
///     let response = ApiCallBuilder::call(
///             method,            
///             url,
///             None, // No custom headers
///             Some(payload)
///         )
///         .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
///         .execute()
///         .await
///         .unwrap();
///
///     dbg!(response);
/// }
/// ```
///
/// Example 2: With Headers
/// ```
/// use serde_json::json;
/// use rgwml::api_utils::ApiCallBuilder;
///
/// #[tokio::main]
/// async fn main() {
///     let method = "POST"; // Or "GET"
///     let url = "http://example.com/api/submit";
///     let headers = json!({
///         "Content-Type": "application/json",
///         "Authorization": "Bearer your_token_here"
///     });
///     let payload = json!({
///         "field1": "Hello",
///         "field2": 123
///     });
///     let response = ApiCallBuilder::call(
///             method,
///             url,
///             Some(headers), // Custom headers
///             Some(payload)
///         )
///         .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
///         .execute()
///         .await
///         .unwrap();
///
///     dbg!(response);
/// }
/// ```
/// Example 3: With application/x-www-form-urlencoded Content-Type
/// ```
/// use serde_json::json;
/// use rgwml::api_utils::ApiCallBuilder;
/// use std::collections::HashMap;
///
/// #[tokio::main]
/// async fn main() {
///     let method = "POST"; // Or "GET"
///     let url = "http://example.com/api/submit";
///     let headers = json!({
///         "Content-Type": "application/x-www-form-urlencoded"
///     });
///     let payload = json!({
///         "field1": "value1",
///         "field2": "value2"
///     });
///     let response = ApiCallBuilder::call(
///             method,            
///             url,
///             Some(headers),
///             Some(payload) // Payload as form data
///         )
///         .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
///         .execute()
///         .await
///         .unwrap();
///
///     dbg!(response);
/// }
/// ```
///
/// These examples demonstrate how to use the ApiCallBuilder with and without custom headers.
/// Since the headers and payload are specified as `serde_json::Value`, it offers flexibility in
/// constructing various types of requests.
///
/// # Note
///
/// Be cautious when caching POST requests, as they typically send unique data each time.
/// Caching is most effective when the same request is likely to yield the same response.

pub struct ApiCallBuilder {
    method: String,
    url: String,
    header_option: Option<JsonValue>,
    payload: Option<JsonValue>,
    cache_duration: Option<u64>,
    cache_path: Option<String>,
}

impl ApiCallBuilder {

        pub fn get_docs() -> String {
        let docs = r#"

++++++++++++++++++++++++++++++++
+> ApiCallBuilder Documentation <+
++++++++++++++++++++++++++++++++

    use serde_json::json;
    use rgwml::api_utils::ApiCallBuilder;
    use std::collections::HashMap;

    #[tokio::main]
    async fn main() {
        // Fetch and cache post request without headers
        let response = fetch_and_cache_post_request().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch data: {}", e);
            std::process::exit(1);
        });
        println!("Response: {:?}", response);

        // Fetch and cache post request with headers
        let response_with_headers = fetch_and_cache_post_request_with_headers().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch data with headers: {}", e);
            std::process::exit(1);
        });
        println!("Response with headers: {:?}", response_with_headers);

        // Fetch and cache post request with form URL encoded content type
        let response_form_urlencoded = fetch_and_cache_post_request_form_urlencoded().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch form URL encoded data: {}", e);
            std::process::exit(1);
        });
        println!("Form URL encoded response: {:?}", response_form_urlencoded);
    }

    // Example 1: Without Headers
    async fn fetch_and_cache_post_request() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let payload = json!({
            "field1": "Hello",
            "field2": 123
        });

        let response = ApiCallBuilder::call(method, url, None, Some(payload))
            .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
            .execute()
            .await?;

        Ok(response)
    }

    // Example 2: With Headers
    async fn fetch_and_cache_post_request_with_headers() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let headers = json!({
            "Content-Type": "application/json",
            "Authorization": "Bearer your_token_here"
        });
        let payload = json!({
            "field1": "Hello",
            "field2": 123
        });

        let response = ApiCallBuilder::call(method, url, Some(headers), Some(payload))
            .maintain_cache(30, "/path/to/post_with_headers_cache.json") // Uses cache for 30 minutes
            .execute()
            .await?;

        Ok(response)
    }

    // Example 3: With application/x-www-form-urlencoded Content-Type
    async fn fetch_and_cache_post_request_form_urlencoded() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let headers = json!({
            "Content-Type": "application/x-www-form-urlencoded"
        });
        let payload = HashMap::from([
            ("field1", "value1"),
            ("field2", "value2"),
        ]);

        let response = ApiCallBuilder::call(method, url, Some(headers), Some(payload))
            .maintain_cache(30, "/path/to/post_form_urlencoded_cache.json") // Uses cache for 30 minutes
            .execute()
            .await?;

        Ok(response)
    }

"#;
        // docs.to_string();

        println!("{}", docs.to_string());

        docs.to_string()
    }



    pub fn call(
        method: &str,
        url: &str,
        header_option: Option<JsonValue>,
        payload: Option<JsonValue>,
    ) -> Self {
        Self {
            method: method.to_uppercase(),
            url: url.to_string(),
            header_option,
            payload,
            cache_duration: None,
            cache_path: None,
        }
    }

    pub fn maintain_cache(mut self, minutes: u64, path: &str) -> Self {
        self.cache_duration = Some(minutes);
        self.cache_path = Some(path.to_string());
        self
    }

    pub async fn execute(self) -> Result<String, Box<dyn StdError>> {
        if let Some(cache_path) = &self.cache_path {
            if let Ok(metadata) = fs::metadata(cache_path) {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(duration) = modified.elapsed() {
                        if duration.as_secs() / 60 < self.cache_duration.unwrap_or(0) {
                            println!("Fetching data from cache.");
                            return fs::read_to_string(cache_path)
                                .map_err(|e| Box::new(e) as Box<dyn StdError>);
                        }
                    }
                }
            }
        }

        println!("Making a new API call.");
        let reqwest_method = match self.method.as_str() {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            // ... handle other cases or default case ...
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid HTTP method",
                )))
            }
        };

        let client = Client::new();
        let mut request_builder = client.request(reqwest_method, &self.url);

        let mut is_form = false;
        // Determine if Content-Type is application/x-www-form-urlencoded
        let is_form_content_type = if let Some(ref header_json) = self.header_option {
            //let mut is_form = false;

            let header_map: HeaderMap = header_json
                .as_object()
                .unwrap_or(&Map::new())
                .iter()
                .map(|(k, v)| {
                    let header_name = HeaderName::from_str(k).unwrap();
                    let header_value = HeaderValue::from_str(v.as_str().unwrap()).unwrap();

                    // Check Content-Type here
                    if k == "Content-Type" && v == "application/x-www-form-urlencoded" {
                        is_form = true;
                    }

                    (header_name, header_value)
                })
                .collect();

            request_builder = request_builder.headers(header_map);
            is_form
        } else {
            false
        };

        let payload_clone = self.payload.clone();

        match self.method.as_str() {
            "GET" => {
                if let Some(query_params_json) = payload_clone {
                    // Use query_params_json here for GET request
                    let query_params = query_params_json
                        .as_object()
                        .ok_or("Invalid query parameters format")?
                        .iter()
                        .map(|(k, v)| (k, v.to_string()))
                        .collect::<HashMap<_, _>>();
                    request_builder = request_builder.query(&query_params);
                }
            }
            "POST" | "PUT" => {
                if let Some(body_json) = self.payload.clone() {
                    // Use body_json here for POST or PUT request
                    if is_form_content_type {
                        // Convert JSON payload to form data if Content-Type is application/x-www-form-urlencoded
                        let form_data: HashMap<String, String> = serde_json::from_value(body_json)
                            .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
                        request_builder = request_builder.form(&form_data);
                    } else {
                        // Use JSON payload
                        request_builder = request_builder.json(&body_json);
                    }
                }
            }
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Unsupported HTTP method",
                )))
            }
        }

        // Convert JsonValue to HeaderMap
        if let Some(header_json) = self.header_option {
            let header_map: HeaderMap = header_json
                .as_object()
                .unwrap_or(&Map::new())
                .iter()
                .map(|(k, v)| {
                    let header_name = HeaderName::from_str(k).unwrap();
                    let header_value = HeaderValue::from_str(v.as_str().unwrap()).unwrap();
                    (header_name, header_value)
                })
                .collect();
            request_builder = request_builder.headers(header_map);
        }

        // Handle payload for POST, PUT based on Content-Type
        if ["POST", "PUT"].contains(&self.method.as_str()) {
            if let Some(payload_json) = self.payload {
                if is_form_content_type {
                    // Convert JSON payload to form data if Content-Type is application/x-www-form-urlencoded
                    let form_data: HashMap<String, String> =
                        serde_json::from_value(payload_json)
                            .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
                    request_builder = request_builder.form(&form_data);
                    dbg!(&is_form, &form_data);
                } else {
                    // Use JSON payload
                    request_builder = request_builder.json(&payload_json);
                }
            }
        }

        //dbg!(&request_builder);

        // Send the request and process the response
        let response: Response = request_builder
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn StdError>)?;

        let response_text = if response.status().is_success() {
            response
                .text()
                .await
                .map_err(|e| Box::new(e) as Box<dyn StdError>)?
        } else {
            return Err(Box::new(response.error_for_status().unwrap_err()) as Box<dyn StdError>);
        };

        // Write response to cache if needed
        if let Some(cache_path) = &self.cache_path {
            fs::write(cache_path, &response_text).map_err(|e| Box::new(e) as Box<dyn StdError>)?;
        }

        Ok(response_text)
    }
}
