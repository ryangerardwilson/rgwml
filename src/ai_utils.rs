//ai_utils.rs
use futures::future::join_all;
use fuzzywuzzy::fuzz;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

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
    //neural_associations_dataframe: DataFrame,
    //neural_associations: &'a [NeuralAssociations2D],
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

        for row in data_arc.as_ref().iter() {
            let input_word_count = row.input.split_whitespace().count() as i32;
            let items_to_process_lower = items_to_process.to_lowercase();

            let similarity = fuzz::ratio(&items_to_process_lower, &row.input);

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

        let best_result = results.clone().into_iter().max_by(|a, b| {
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
/// ```
/// let config = NeuralAssociations2DDataFrameConfig {
///     input_column: "address",
///     output_column: "name",
/// };
/// ```
pub struct NeuralAssociations2DDataFrameConfig {
    pub input_column: &'static str,
    pub output_column: &'static str,
}


