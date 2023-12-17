// api_utils.rs
use reqwest::{Client, Method, Response};
use reqwest::header::{HeaderMap, HeaderValue, HeaderName};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::fs;
use std::str::FromStr;
use std::error::Error as StdError;

pub enum HeaderOption {
    None,
    Set(JsonValue)
}

/// A flexible builder for sending and caching API requests.
///
/// This struct provides a fluent interface to build API requests with support for method
/// chaining. It features an option for including custom headers via the `HeaderOption` enum.
/// The `HeaderOption::Set` variant takes a `JsonValue`, allowing you to specify headers in
/// JSON format. These headers are then converted and inserted into the request.
///
/// If caching is enabled, responses are stored and reused for subsequent requests made
/// within the specified cache duration.
///
/// Example 1: Without Headers
/// ```
/// use reqwest::Method;
/// use serde_json::json;
/// use rgwml::api_utils::{ApiCallBuilder, HeaderOption};
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
///             HeaderOption::None, // No custom headers
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
/// use rgwml::api_utils::{ApiCallBuilder, HeaderOption};
///
/// #[tokio::main]
/// async fn main() {
///     let url = "http://example.com/api/submit";
///     let headers = json!({
///         "Content-Type": "application/json",
///         "Authorization": "Bearer token123"
///     });
///     let payload = json!({
///         "field1": "Hello",
///         "field2": 123
///     });
///     let response = ApiCallBuilder::call(
///             Method::POST,
///             url,
///             HeaderOption::Set(headers), // Custom headers set
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
/// In the second example, the `HeaderOption::Set` variant is used to specify custom headers
/// in JSON format. These headers are processed and applied to the request.
///
/// # Note
///
/// Be cautious when caching POST requests, as they typically send unique data each time.
/// Caching is most effective when the same request is likely to yield the same response.
/// ```

pub struct ApiCallBuilder<T: Serialize> {
    method: Method,
    url: String,
    header_option: HeaderOption,
    payload: Option<T>,
    cache_duration: Option<u64>,
    cache_path: Option<String>,
}

impl<T: Serialize> ApiCallBuilder<T> {

    pub fn call(method: Method, url: &str, header_option: HeaderOption, payload: Option<T>) -> Self {
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

                match self.header_option {
            HeaderOption::None => {}
            HeaderOption::Set(headers) => {
                // Clone the headers as HeaderMap
let header_map = headers
    .as_object()
    .map(|headers_obj| {
        let mut map = HeaderMap::new();
        for (key, value) in headers_obj.iter() {
            if let Some(value_str) = value.as_str() {
                let key_str = key.to_string();
                let value_header = HeaderValue::from_str(value_str).unwrap();
                let key_header = HeaderName::from_str(&key_str).unwrap(); // Convert String to HeaderName
                map.insert(key_header, value_header); // Insert into map
            }
        }
        map
    })
    .unwrap_or_default();

request_builder = request_builder.headers(header_map);

            }
        }


        if self.method == Method::POST {
            if let Some(payload) = self.payload {
                request_builder = request_builder.json(&payload);
            }
        }

        let response: Response = request_builder.send().await
            .map_err(|e| Box::new(e) as Box<dyn StdError>)?;

        let response_text = if response.status().is_success() {
            response.text().await.map_err(|e| Box::new(e) as Box<dyn StdError>)?
        } else {
            return Err(Box::new(response.error_for_status().unwrap_err()) as Box<dyn StdError>);
        };

        if let Some(cache_path) = &self.cache_path {
            fs::write(cache_path, &response_text)
                .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
        }

        Ok(response_text)
    }
}



