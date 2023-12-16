use reqwest::header::HeaderMap;
use reqwest::{Client, Error, Method, Response};
use serde::Serialize;

pub enum HeaderOption {
    None,
    Custom(HeaderMap),
}

/// Executes an HTTP request with specified options.
///
/// This asynchronous function sends an HTTP request using the provided method, URL,
/// headers, and optional payload. It is designed to be flexible, allowing for various
/// HTTP methods and custom headers.
///
/// # Parameters
/// - `method`: The HTTP method (`Method`) to be used for the request, such as GET, POST, etc.
/// - `url`: A string slice (`&str`) representing the URL to which the request is sent.
/// - `header_option`: An enum `HeaderOption` that specifies if headers are to be included. It can be `None` or `Custom(HeaderMap)`.
/// - `payload`: An optional generic parameter (`Option<T>`) representing the request payload. The type `T` must implement the `Serialize` trait.
///
/// # Returns
/// Returns `Result<String, Error>`. On success, it returns the response body as a `String`. On failure, it returns an `Error`.
///
/// # Examples
/// ```
/// use reqwest::{Method};
/// use your_crate::{call_api, HeaderOption};
///
/// #[tokio::main]
/// async fn main() {
///     let url = "http://example.com";
///     let result = call_api(Method::GET, url, HeaderOption::None, None::<()>).await;
///     match result {
///         Ok(response) => println!("Response: {}", response),
///         Err(e) => println!("Error: {}", e),
///     }
/// }
/// ```
///
/// # Errors
/// Returns an error if the HTTP request fails, for example due to network issues, or if the server response indicates an error (non-2xx status codes).
///
pub async fn call_api<T: Serialize>(
    method: Method,
    url: &str,
    header_option: HeaderOption,
    payload: Option<T>,
) -> Result<String, Error> {
    fn header_option_to_header_map(header_option: HeaderOption) -> Option<HeaderMap> {
        match header_option {
            HeaderOption::None => None,
            HeaderOption::Custom(headers) => Some(headers),
        }
    }

    let client = Client::new();
    // let mut request_builder = client.request(method, url);
    let mut request_builder = client.request(method.clone(), url);

    // Convert HeaderOption to HeaderMap if needed
    if let Some(headers_map) = header_option_to_header_map(header_option) {
        request_builder = request_builder.headers(headers_map);
    }

    // Check if method is POST and payload is provided
    if Method::POST == method && payload.is_some() {
        request_builder = request_builder.json(&payload.unwrap());
    }

    // Send the request and await the response
    let response: Response = request_builder.send().await?;

    if response.status().is_success() {
        response.text().await.map_err(From::from)
    } else {
        Err(response.error_for_status().unwrap_err())
    }
}
