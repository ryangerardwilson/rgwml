// api_utils.rs
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, RequestBuilder, Response};
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fs;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

pub struct ApiCallBuilder {
    method: String,
    url: String,
    header_option: Option<JsonValue>,
    payload: Option<JsonValue>,
    cache_duration: Option<u64>,
    cache_path: Option<String>,
    retry_count: usize,
    retry_timeout: u64,
}

impl ApiCallBuilder {
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
            retry_count: 0,
            retry_timeout: 1,
        }
    }

    pub fn maintain_cache(mut self, minutes: u64, path: &str) -> Self {
        self.cache_duration = Some(minutes);
        self.cache_path = Some(path.to_string());
        self
    }

    pub fn retries(mut self, count: usize, timeout: u64) -> Self {
        self.retry_count = count;
        self.retry_timeout = timeout;
        self
    }

    #[allow(unused_assignments)]
    pub async fn execute(self) -> Result<String, Box<dyn StdError>> {
        async fn try_execute(request_builder: RequestBuilder) -> Result<String, Box<dyn StdError>> {
            let response: Response = request_builder
                .send()
                .await
                .map_err(|e| Box::new(e) as Box<dyn StdError>)?;

            if response.status().is_success() {
                let response_text = response
                    .text()
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
                Ok(response_text)
            } else {
                Err(Box::new(response.error_for_status().unwrap_err()) as Box<dyn StdError>)
            }
        }

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

        let mut attempts = 0;
        let mut final_response_text: String = String::new();

        loop {
            //println!("Attempt: {}", attempts + 1);

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
                            let form_data: HashMap<String, String> =
                                serde_json::from_value(body_json)
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
            if let Some(ref header_json) = self.header_option {
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
                if let Some(ref payload_json) = self.payload {
                    if is_form_content_type {
                        // Convert JSON payload to form data if Content-Type is application/x-www-form-urlencoded
                        let form_data: HashMap<String, String> =
                            serde_json::from_value(payload_json.clone())
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

            //println!("Attempt: {}", attempts + 1);
            match try_execute(request_builder).await {
                Ok(response_text) => {
                    final_response_text = response_text; // Capture the successful response
                    break; // Exit loop on success
                }
                Err(e) if attempts < self.retry_count => {
                    println!(
                        "Error: {}. Retrying in {} seconds...",
                        e, self.retry_timeout
                    );
                    sleep(Duration::from_secs(self.retry_timeout)).await;
                    attempts += 1;
                }
                Err(e) => return Err(e),
            }
        }

        // Handle cache writing here, using `final_response_text`
        if let Some(cache_path) = &self.cache_path {
            fs::write(cache_path, &final_response_text)
                .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
        }

        Ok(final_response_text)
    }
}
