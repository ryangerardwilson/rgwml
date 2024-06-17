//ai_utils.rs
use crate::api_utils::ApiCallBuilder;
use crate::csv_utils::CsvBuilder;
use futures::future::join_all;
use fuzzywuzzy::fuzz;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs::File;
// use serde_json::Value;
use reqwest::multipart;
use reqwest::Client;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::io::copy;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use tempfile::NamedTempFile;

/// The `NeuralAssociations2D` struct represents a basic structure for a two-dimensional neural network model.
/// It consists of two primary fields: `input` and `output`. These fields are designed to work with string representations
/// of data, which must be pre-processed or encoded into a suitable format for neural network processing.
#[derive(Clone, Debug, Deserialize)]
pub struct NeuralAssociations2D {
    pub input: String,
    pub output: String,
}

/// `SimilarityResult` holds the result of a similarity comparison between a pair of text strings.
/// It is used to store the outcome of processing each text permutation against the training data
/// in the neural network.
///
/// Fields:
/// - `similarity`: A floating-point value representing the similarity score between the input text and the training data.
/// - `input`: A string that holds the original input text that was compared.
/// - `output`: A string that holds the corresponding output from the training data for the given input.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SimilarityResult {
    pub similarity: f32,
    pub input: String,
    pub output: String,
}

/// `ShowComplications` is an enum used to control the verbosity of output during the execution of functions.
/// It is particularly useful for debugging and analyzing the detailed workings of the neural network.
///
/// Variants:
/// - `True`: Enables detailed logging, showing inner decisions and computations of the function.
/// - `False`: Disables detailed logging, resulting in a cleaner and more concise output.
pub enum ShowComplications {
    True,
    False,
}

/// `SplitUpto` defines the granularity of text segmentation for processing and analysis.
/// It specifies the minimal size of text permutations to be considered during neural network processing.
///
/// Variant:
/// - `WordSetLength(usize)`: Specifies the minimal number of consecutive words to be included in each text segment.
///   For example, `WordSetLength(3)` means the text will be split into segments of at least three consecutive words.
pub enum SplitUpto {
    WordSetLength(usize),
}

/// `WordLengthSensitivity` provides a mechanism to adjust the neural network's sensitivity
/// to the length of words in its processing. This can be crucial in scenarios where the size or length
/// of textual input significantly impacts the network's performance or accuracy.
///
/// Variants:
/// - `None`: No sensitivity to word length. The network will not adjust its calculations based on the
///   length of words. This is suitable for scenarios where word length is not a critical factor.
/// - `Coefficient(f32)`: A floating-point value between 0 and 1 to set the degree of sensitivity. A
///   coefficient of 0 implies no sensitivity, while 1 implies maximum sensitivity. The coefficient
///   scales the impact of word length differences on the network's processing, allowing for nuanced
///   adjustments tailored to specific use cases.
pub enum WordLengthSensitivity {
    None,
    Coefficient(f32),
}

/// `fuzzai` is an asynchronous function designed to process a batch of
/// neural associations in parallel, applying a two-dimensional analysis based on the provided parameters.
/// This function helps in understanding the inner workings of the neural network's
/// decision-making process in a concurrent environment.
pub async fn fuzzai<'a>(
    csv_file_path: &str,
    input_column: &str,
    output_column: &str,
    text_in_focus: &str,
    task_name: &str,
    split_upto: SplitUpto,
    show_complications: ShowComplications,
    word_length_sensitivity: WordLengthSensitivity,
) -> Result<String, Box<dyn Error>> {
    async fn process_item(
        items_to_process: String,
        data_arc: Arc<&[NeuralAssociations2D]>,
        user_input_length: usize,
        task_name: &str,
        show_complications: &ShowComplications,
        word_length_sensitivity: &WordLengthSensitivity,
    ) -> Option<SimilarityResult> {
        let mut all_futures: Vec<Pin<Box<dyn Future<Output = SimilarityResult>>>> = Vec::new();

        let mut max_similarity: f32 = 0.0;
        let mut max_similarity_word_length = 0;

        for row in data_arc.as_ref().iter() {
            let input_word_count = row.input.split_whitespace().count() as i32;
            let items_to_process_lower = items_to_process.to_lowercase();

            let similarity = fuzz::ratio(&items_to_process_lower, &row.input) as f32;

            let current_longest_word_length = row
                .input
                .split_whitespace()
                .map(str::len)
                .max()
                .unwrap_or(0);

            if similarity > max_similarity {
                max_similarity = similarity as f32;
                max_similarity_word_length = current_longest_word_length;
            }

            match word_length_sensitivity {
                WordLengthSensitivity::None => {
                    // No adjustment for word length differences
                    // let adjusted_similarity = similarity as f32;
                    if let ShowComplications::True = show_complications {
                        println!(
                            "Comparing ({}) '{}' with '{}' - Similarity: {}",
                            &task_name, &items_to_process, &row.input, similarity
                        );
                    }
                    all_futures.push(Box::pin(async move {
                        SimilarityResult {
                            similarity: similarity as f32,
                            input: row.input.to_string(),
                            output: row.output.to_string(),
                        }
                    }));
                }
                WordLengthSensitivity::Coefficient(coefficient) => {
                    // Ensure coefficient is within the range 0 to 1
                    let clamped_coefficient = coefficient.clamp(0.0, 1.0);
                    let word_count_diff =
                        (user_input_length as i32 - input_word_count as i32).abs() as f32;
                    let adjustment_factor = clamped_coefficient * word_count_diff;
                    let adjusted_similarity = (similarity as f32) - adjustment_factor;

                    if let ShowComplications::True = show_complications {
                        println!(
                "Comparing ({}) '{}' with '{}' - Similarity: {}; Word Count Adjustment: {}",
                &task_name, &items_to_process, &row.input, similarity, adjusted_similarity
            );
                    }

                    all_futures.push(Box::pin(async move {
                        SimilarityResult {
                            similarity: adjusted_similarity as f32,
                            input: row.input.to_string(),
                            output: row.output.to_string(),
                        }
                    }));
                }
            }
        }

        let results = join_all(all_futures).await;

        let mut final_results: Vec<SimilarityResult> = Vec::new();
        for result in results {
            let longest_word_length = result
                .input
                .split_whitespace()
                .map(str::len)
                .max()
                .unwrap_or(0);

            if longest_word_length == max_similarity_word_length && result.similarity > 80.0 {
                final_results.push(SimilarityResult {
                    similarity: 100.0,
                    input: result.input.clone(),
                    output: result.output.clone(),
                });
            }

            /*
            else {
                final_results.push(SimilarityResult {
                    similarity: result.similarity * 0.9,
                    input: result.input.clone(),
                    output: result.output.clone(),
                });
            }
            */
        }

        let best_result = final_results.into_iter().max_by(|a, b| {
            a.similarity
                .partial_cmp(&b.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        best_result
    }

    async fn process_message_concurrently(
        message: &str,
        data_arc: Arc<&[NeuralAssociations2D]>,
        split_upto: &SplitUpto,
        task_name: &str,
        show_complications: &ShowComplications,
        word_length_sensitivity: &WordLengthSensitivity,
    ) -> Result<String, Box<dyn Error>> {
        // Create a regex to match non-alphabetic characters
        let re = Regex::new(r"[^a-zA-Z\s]").unwrap();

        // Replace all non-alphabetic characters with nothing
        let message_str = re.replace_all(message, "");

        let split_words: Vec<&str> = message_str.split_whitespace().collect();

        // let split_words: Vec<&str> = message.split_whitespace().collect();
        let user_input_length = split_words.len();
        let mut words = Vec::new();

        let min_chunk_size = match split_upto {
            SplitUpto::WordSetLength(size) => {
                // Dereference `size` for comparison

                if *size == 0 || *size >= user_input_length {
                    user_input_length // Consider the entire user input
                } else {
                    *size // Dereference to match the type of user_input_length
                }
            }
        };

        for chunk_size in min_chunk_size..=user_input_length {
            for start in 0..split_words.len() {
                let end = std::cmp::min(start + chunk_size, split_words.len());
                if end > start {
                    // Adjusted condition to allow single-word chunks
                    words.push(split_words[start..end].join(" "));
                }
            }
        }

        let total_arc_items = data_arc.len();
        let total_combinations = words.len();

        // If you want to calculate tasks considering both word combinations and dictionary entries
        let total_concurrent_tasks = total_combinations * total_arc_items;

        if let ShowComplications::True = show_complications {
            println!(
                "Total concurrent ({} tasks): {}",
                task_name, total_concurrent_tasks
            );
        }

        let word_and_phrase_futures: Vec<_> = words
            .iter()
            .map(|word| {
                let data_arc_clone = Arc::clone(&data_arc);
                process_item(
                    word.clone(),
                    data_arc_clone,
                    user_input_length,
                    &task_name,
                    &show_complications,
                    &word_length_sensitivity,
                )
            })
            .collect();

        let results = join_all(word_and_phrase_futures).await;

        // dbg!(&results);

        let mut max_similarity_per_output: HashMap<String, SimilarityResult> = HashMap::new();

        // dbg!(&results);
        for result_option in results {
            if let Some(result) = result_option {
                let output = result.output.clone();
                if let Some(entry) = max_similarity_per_output.get_mut(&output) {
                    if result.similarity > entry.similarity {
                        *entry = result.clone();
                    }
                } else {
                    max_similarity_per_output.insert(output, result.clone());
                }
            }
        }

        let filtered_results: Vec<_> = max_similarity_per_output.values().cloned().collect();

        //dbg!(&filtered_results);

        let mut sorted_filtered_results = filtered_results;
        sorted_filtered_results.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let top_results = sorted_filtered_results
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();

        //dbg!(&top_results);

        if !top_results.is_empty() {
            // Serialize the top results into a JSON string
            let json = serde_json::to_string(&top_results)?;
            Ok(json)
        } else {
            Err("No suitable results found for Parallel call type".into())
        }
    }

    /*
    let neural_associations: Vec<NeuralAssociations2D> = neural_associations_dataframe
        .into_iter()
        .map(|row| serde_json::from_value(serde_json::to_value(row).unwrap()).unwrap())
        .collect();
    */

    let mut rdr = csv::Reader::from_path(csv_file_path)?;
    let mut neural_associations = Vec::new();

    // Create a regex to match non-alphabetic and non-whitespace characters
    let re = Regex::new(r"[^a-zA-Z\s]").unwrap();

    for result in rdr.deserialize() {
        let record: HashMap<String, String> = result?;

        let input_raw = record.get(input_column).ok_or("Input column not found")?;
        let input = re.replace_all(input_raw, "").to_string();

        let output_raw = record.get(output_column).ok_or("Output column not found")?;
        let output = re.replace_all(output_raw, "").to_string();

        neural_associations.push(NeuralAssociations2D { input, output });
    }

    let data_arc = Arc::new(neural_associations);

    let mut futures = Vec::new();

    let data_arc_clone = Arc::clone(&data_arc);
    let boxed_slice: Box<[NeuralAssociations2D]> = data_arc_clone.to_vec().into_boxed_slice();

    let raw_slice: *const [NeuralAssociations2D] = Box::leak(boxed_slice);
    let static_slice: &'static [NeuralAssociations2D] = unsafe { &*raw_slice };

    let data_slice: Arc<&[NeuralAssociations2D]> = Arc::new(static_slice);

    //let data_arc_clone = Arc::clone(&data_arc);
    let future = process_message_concurrently(
        text_in_focus,
        Arc::clone(&data_slice),
        //data_arc_clone,
        &split_upto,
        &task_name,
        &show_complications,
        &word_length_sensitivity,
    );
    futures.push(future);

    let results = join_all(futures).await;

    //let mut most_similar_result = None;
    let mut most_similar_json = String::new();
    let mut max_similarity = 0.0;

    for result in results.iter() {
        if let Ok(json_string) = result {
            if let Ok(parsed_results) = serde_json::from_str::<Vec<SimilarityResult>>(json_string) {
                if let Some(similarity_result) = parsed_results.first() {
                    if similarity_result.similarity > max_similarity {
                        max_similarity = similarity_result.similarity;
                        most_similar_json = json_string.clone();
                    }
                }
            }
        }
    }

    if !most_similar_json.is_empty() {
        // dbg!(&most_similar_json);
        // Return the JSON string of the most similar result
        Ok(most_similar_json)
    } else {
        Ok("{}".to_string())
    }
}

/// Configuration for mapping a DataFrame to a `NeuralAssociations2D` DataFrame.
///
/// This struct is used to specify the column names in the original DataFrame
/// that correspond to the `input` and `output` fields of the `NeuralAssociations2D` structure.
///
/// # Fields
/// * `input_column` - The name of the column in the original DataFrame to use as the `input`.
/// * `output_column` - The name of the column in the original DataFrame to use as the `output`.
///
/// # Examples
///
///
/// let config = NeuralAssociations2DDataFrameConfig {
///     input_column: "address",
///     output_column: "name",
/// };
///
pub struct NeuralAssociations2DDataFrameConfig {
    pub input_column: &'static str,
    pub output_column: &'static str,
}

pub async fn fetch_and_print_openai_batches(api_key: &str) -> Result<CsvBuilder, Box<dyn Error>> {
    let client = Client::new();
    let url = "https://api.openai.com/v1/batches?limit=12".to_string();

    let response = client
        .get(&url)
        .bearer_auth(api_key)
        .header("Content-Type", "application/json")
        .send()
        .await?;

    let response_json: Value = response.json().await?;

    let headers = vec![
        "batch_id".to_string(),
        "description".to_string(),
        "status".to_string(),
        "completed".to_string(),
        "failed".to_string(),
        "total".to_string(),
        "error".to_string(),
        "output_file_id".to_string(),
    ];

    let mut data = vec![];

    if let Some(batches) = response_json["data"].as_array() {
        //dbg!(&batches);
        for batch in batches {
            let batch_id = batch["id"].as_str().unwrap_or("No ID").to_string();
            let description = batch["metadata"]["batch_description"]
                .as_str()
                .unwrap_or("No description")
                .to_string();
            let status = batch["status"].as_str().unwrap_or("No status").to_string();
            let completed = batch["request_counts"]["completed"]
                .as_i64()
                .unwrap_or(0)
                .to_string();
            let failed = batch["request_counts"]["failed"]
                .as_i64()
                .unwrap_or(0)
                .to_string();
            let total = batch["request_counts"]["total"]
                .as_i64()
                .unwrap_or(0)
                .to_string();

            let error = batch["errors"]["data"]
                .as_array()
                .and_then(|errors| errors.get(0))
                .and_then(|error| error["message"].as_str())
                .unwrap_or("No error")
                .to_string();

            let output_file_id = batch["output_file_id"]
                .as_str()
                .unwrap_or("No ID")
                .to_string();

            data.push(vec![
                batch_id,
                description,
                status,
                completed,
                failed,
                total,
                error,
                output_file_id,
            ]);
        }
    } else {
        println!("No batches found.");
    }

    /*
    let csv_builder = CsvBuilder::from_raw_data(headers, data).add_column_header("id").resequence_id_column("id").cascade_sort(vec![("id".to_string(), "DESC".to_string())]).drop_columns(vec!["id"]);

    let return_builder = CsvBuilder::from_copy(&csv_builder);

    let _ = CsvBuilder::from_copy(&csv_builder).print_rows().drop_columns(vec!["batch_id", "output_file_id"]).print_table_all_rows();
    */

    // Breaking down the chained method calls into intermediate steps
    //let csv_builder = CsvBuilder::from_raw_data(headers, data).reverse_rows();

    let mut csv_builder_initial = CsvBuilder::from_raw_data(headers, data);
    let csv_builder_with_header = csv_builder_initial.add_column_header("id");
    let csv_builder_resequenced = csv_builder_with_header.resequence_id_column("id");
    let csv_builder_sorted =
        csv_builder_resequenced.cascade_sort(vec![("id".to_string(), "DESC".to_string())]);
    let csv_builder_unordered = csv_builder_sorted.resequence_id_column("id");
    let csv_builder = csv_builder_unordered.order_columns(vec!["id", "..."]);

    let return_builder = CsvBuilder::from_copy(&csv_builder);

    let _ = CsvBuilder::from_copy(&csv_builder)
        .print_rows()
        .drop_columns(vec!["batch_id", "output_file_id"])
        .print_table_all_rows();

    //csv_builder.print_rows().print_table_all_rows();
    Ok(return_builder)
}

pub async fn get_openai_analysis_json(
    text_to_be_analyzed: &str,
    analysis_query: HashMap<String, String>,
    api_key: &str,
    model: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    //dbg!(&text_to_be_analyzed, &analysis_query, &api_key, &model);
    let mut instruction = "Your only job is to extract information from the user's content and render a json in the below format: {".to_owned();
    let mut meanings =
        " Your response should take into account the following meanings of the keys:\n".to_owned();
    let mut counter = 1;

    for (key, value) in &analysis_query {
        instruction += &format!(" '{}': 'your response',", key);
        meanings += &format!(" {}. {}: {}\n", counter, key, value);
        counter += 1;
    }
    instruction.pop(); // Remove the last comma
    instruction += "},"; // Close the format part
    instruction += &meanings;
    instruction += " with each response being less than 12 words.";

    //dbg!(&instruction, &text_to_be_analyzed);

    let method = "POST";
    let url = "https://api.openai.com/v1/chat/completions";
    let headers = json!({
        "Content-Type": "application/json",
        "Authorization": format!("Bearer {}", api_key)
    });

    let mut messages = vec![
        json!({
            "role": "system",
            "content": instruction
        }),
        json!({
            "role": "user",
            "content": text_to_be_analyzed
        }),
    ];

    for (key, value) in analysis_query {
        messages.push(json!({
            "role": "user",
            "content": format!("{}: {}", key, value)
        }));
    }

    let response_format = json!({"type": "json_object"});

    let payload = json!({
        "model": model,
        "response_format": response_format,
        "messages": messages,
    });

    let mut attempts = 0;
    const MAX_ATTEMPTS: usize = 3;

    while attempts < MAX_ATTEMPTS {
        attempts += 1;
        let response =
            match ApiCallBuilder::call(method, url, Some(headers.clone()), Some(payload.clone()))
                .retries(3, 2)
                .execute()
                .await
            {
                Ok(response) => response,
                Err(_) => {
                    if attempts >= MAX_ATTEMPTS {
                        return Err("Failed to make API call".into());
                    }
                    continue; // Try again
                }
            };

        let parsed: Value = match serde_json::from_str(&response) {
            Ok(parsed) => parsed,
            Err(_) => {
                if attempts >= MAX_ATTEMPTS {
                    return Err("Failed to parse API response".into());
                }
                continue; // Try again
            }
        };

        if let Some(choices) = parsed["choices"].as_array() {
            if let Some(first_choice) = choices.first() {
                let content_str = first_choice["message"]["content"]
                    .as_str()
                    .ok_or("No content string found")?;
                //dbg!(&content_str);

                let content_value: Value = serde_json::from_str(content_str)?;
                //dbg!(&content_value);

                let mut content_map = HashMap::new();

                if let Value::Object(map) = content_value {
                    for (key, value) in map {
                        let value_str = match value {
                            Value::String(s) => s,
                            Value::Bool(b) => b.to_string(),
                            Value::Number(n) => n.to_string(),
                            _ => value.to_string(),
                        };
                        content_map.insert(key, value_str);
                        // dbg!(&content_map);
                    }
                } else {
                    if attempts >= MAX_ATTEMPTS {
                        return Err("Failed to extract content data".into());
                    }
                    continue; // Try again
                }

                return Ok(content_map);
            }
        }
    }

    Err("Failed to extract content data".into())
}

pub async fn upload_file_to_openai(
    file_path: &str,
    api_key: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let url = "https://api.openai.com/v1/files";
    let mut file = File::open(file_path)?;
    let mut file_content = Vec::new();
    file.read_to_end(&mut file_content)?;

    let form = multipart::Form::new().text("purpose", "batch").part(
        "file",
        multipart::Part::bytes(file_content).file_name("openai_batch_requests.jsonl"),
    );

    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .bearer_auth(api_key)
        .multipart(form)
        .send()
        .await?;

    let response_json = response.json().await?;
    Ok(response_json)
}

pub async fn create_openai_batch(
    input_file_id: &str,
    api_key: &str,
    batch_description: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let url = "https://api.openai.com/v1/batches";
    let payload = json!({
        "input_file_id": input_file_id,
        "endpoint": "/v1/chat/completions",
        "completion_window": "24h",
        "metadata": {
            "batch_description": batch_description
        }
    });

    //dbg!(&payload);

    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .bearer_auth(api_key)
        .json(&payload)
        .send()
        .await?;

    let response_json = response.json().await?;
    Ok(response_json)
}

pub async fn cancel_openai_batch(
    api_key: &str,
    batch_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let url = format!("https://api.openai.com/v1/batches/{}/cancel", batch_id);

    let response = client
        .post(&url)
        .bearer_auth(api_key)
        .header("Content-Type", "application/json")
        .send()
        .await?;

    let _response_json: Value = response.json().await?;
    //dbg!(&response_json);
    Ok(())
}

pub async fn retrieve_openai_batch(
    api_key: &str,
    file_id: &str,
) -> Result<NamedTempFile, Box<dyn Error>> {
    let url = format!("https://api.openai.com/v1/files/{}/content", file_id);

    let client = Client::new();
    let response = client.get(&url).bearer_auth(api_key).send().await?;

    let mut temp_file = NamedTempFile::new()?;
    copy(&mut response.bytes().await?.as_ref(), &mut temp_file)?;

    Ok(temp_file)
}
