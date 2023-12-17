// api_utils.rs
use reqwest::{Client, Method, Response};
use reqwest::header::{HeaderMap, HeaderValue, HeaderName};
// use serde::Serialize;
use serde_json::{Value as JsonValue, Map};
// use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::error::Error as StdError;

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
/// use reqwest::Method;
/// use serde_json::json;
/// use rgwml::api_utils::ApiCallBuilder;
///
/// #[tokio::main]
/// async fn main() {
///     let url = "http://example.com/api/submit";
///     let payload = json!({
///         "field1": "Hello",
///         "field2": 123
///     });
///     let response = ApiCallBuilder::call(
///             Method::POST,
///             url,
///             None, // No custom headers
///             Some(payload)
///         )
///         .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
///         .execute()
///         .await
///         .unwrap();
///
///     println!("Response from server: {}", response);
/// }
/// ```
///
/// Example 2: With Headers
/// ```
/// use reqwest::Method;
/// use serde_json::json;
/// use rgwml::api_utils::ApiCallBuilder;
///
/// #[tokio::main]
/// async fn main() {
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
///             Method::POST,
///             url,
///             Some(headers), // Custom headers
///             Some(payload)
///         )
///         .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
///         .execute()
///         .await
///         .unwrap();
///
///     println!("Response from server: {}", response);
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
/// ```

pub struct ApiCallBuilder {
    method: Method,
    url: String,
    header_option: Option<JsonValue>,
    payload: Option<JsonValue>,
    cache_duration: Option<u64>,
    cache_path: Option<String>,
}

impl ApiCallBuilder {
    pub fn call(method: Method, url: &str, header_option: Option<JsonValue>, payload: Option<JsonValue>) -> Self {
        Self {
            method,
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
        let client = Client::new();
        let mut request_builder = client.request(self.method.clone(), &self.url);

                // Convert JsonValue to HeaderMap
        if let Some(header_json) = self.header_option {
            let header_map: HeaderMap = header_json.as_object().unwrap_or(&Map::new())
                .iter()
                .map(|(k, v)| {
                    let header_name = HeaderName::from_str(k).unwrap();
                    let header_value = HeaderValue::from_str(v.as_str().unwrap()).unwrap();
                    (header_name, header_value)
                }).collect();
            request_builder = request_builder.headers(header_map);
        }

        // Handle payload
        if self.method == Method::POST {
            if let Some(payload_json) = self.payload {
                request_builder = request_builder.json(&payload_json);
            }
        }

        // Send the request and process the response
        let response: Response = request_builder.send().await
            .map_err(|e| Box::new(e) as Box<dyn StdError>)?;

        let response_text = if response.status().is_success() {
            response.text().await.map_err(|e| Box::new(e) as Box<dyn StdError>)?
        } else {
            return Err(Box::new(response.error_for_status().unwrap_err()) as Box<dyn StdError>);
        };

        // Write response to cache if needed
        if let Some(cache_path) = &self.cache_path {
            fs::write(cache_path, &response_text)
                .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
        }

        Ok(response_text)
    }
}




