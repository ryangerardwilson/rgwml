//ai_utils.rs
use crate::df_utils::DataFrame;
use futures::future::join_all;
use fuzzywuzzy::fuzz;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

//pub type DataFrame = Vec<HashMap<String, Value>>;

/// The `NeuralAssociations2D` struct represents a basic structure for a two-dimensional neural network model.
/// It consists of two primary fields: `input` and `output`. These fields are designed to work with string representations
/// of data, which must be pre-processed or encoded into a suitable format for neural network processing.
///
/// # Examples
///
/// ```rust
/// # use your_crate::NeuralAssociations2D;
/// let neural_network = NeuralAssociations2D {
///     input: "encoded_input_data".to_string(),
///     output: "encoded_output_data".to_string(),
/// };
/// ```
///
/// # Usage
///
/// The `input` field is used to feed in the training data. In the context of a 2D neural network,
/// this data should represent a two-dimensional array, encoded or serialized into a string format.
/// For example, image data or spatially structured data can be converted into a string that preserves
/// the two-dimensional relationships within the data.
///
/// The `output` field is used to store the expected result or the target for the corresponding input.
/// This also should be in an encoded string format that the neural network can interpret and use for training.
///
/// # Training Process
///
/// To train the neural network, a collection of `NeuralAssociations2D` instances should be prepared, each
/// representing a distinct pair of input and expected output. The training process involves adjusting the
/// network parameters to minimize the difference between the actual output of the network and the `output`
/// field in these instances.
///
/// It is important to ensure that the encoding used for both input and output fields maintains the
/// integrity of the two-dimensional data structure, as this is crucial for the effective training and
/// operation of the 2D neural network.
///
/// # Note
///
/// This struct assumes that the necessary pre-processing steps to convert raw data into a suitable string format
/// have been implemented. It is also essential to have a corresponding decoding mechanism to interpret the
/// network's output for practical applications or further analysis.
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
///
/// # Arguments
/// - `neural_associations_dataframe`: A `DataFrame` (Vec<HashMap<String, Value>>), representing the training data.
///   Each `HashMap` should have keys corresponding to the `input` and `output` fields of `NeuralAssociations2D`.
/// - `text_in_focus`: A string reference, typically the text to be analyzed or compared against the training data.
/// - `task_name`: A descriptive name for the task, used primarily for logging or tracking purposes.
/// - `split_upto`: A `SplitUpto` enum to define the minimal size of text permutations for processing.
///   It determines how the input text is split for analysis. For example, if set to `SplitUpto::WordSetLength(2)`,
///   a text "I am a good boy" will be split into "I am", "am a", "a good", "good boy" and all larger permutations.
/// - `show_complications`: A `ShowComplications` enum to control the verbosity of the function. When set to `True`,
///   it prints detailed information about all concurrent decisions made during the function's execution.
/// - `word_length_sensitivity`: A `WordLengthSensitivity` to adjust processing based on word length.
///
/// # Examples
///
/// ## Preparing the DataFrame
/// ```rust
/// let mut data_frame = Vec::new();
/// let mut record = HashMap::new();
/// record.insert("input".to_string(), Value::String("Example input".to_string()));
/// record.insert("output".to_string(), Value::String("Example output".to_string()));
/// data_frame.push(record);
/// ```
///
/// ## Using `None` for `WordLengthSensitivity`
/// ```rust
/// let result = fuzzai(
///     data_frame,
///     "Example text",
///     "Sample Task",
///     SplitUpto::WordSetLength(5),
///     ShowComplications::False,
///     WordLengthSensitivity::None
/// ).await?;
/// ```
///
/// ## Using a `Coefficient` value
/// ```rust
/// let result = fuzzai(
///     data_frame,
///     "Example text",
///     "Sample Task",
///     SplitUpto::WordSetLength(5),
///     ShowComplications::True,
///     WordLengthSensitivity::Coefficient(0.5)
/// ).await?;
/// ```
///
/// # Returns
/// - A `Result` containing a `String` upon success, or an error wrapped in a `Box<dyn Error>` upon failure.
pub async fn fuzzai<'a>(
    neural_associations_dataframe: DataFrame,
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
        let message_str = message;

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

    let neural_associations: Vec<NeuralAssociations2D> = neural_associations_dataframe
        .into_iter()
        .map(|row| serde_json::from_value(serde_json::to_value(row).unwrap()).unwrap())
        .collect();

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

/// Converts a given DataFrame to a `NeuralAssociations2D` DataFrame.
///
/// This function takes a DataFrame and a `NeuralAssociations2DDataFrameConfig`,
/// and creates a new DataFrame where each record is a HashMap representing a `NeuralAssociations2D` object.
/// The `input` and `output` fields of each object are filled based on the specified columns
/// in the original DataFrame.
///
/// # Arguments
/// * `data_frame` - The original DataFrame to convert.
/// * `config` - Configuration specifying which columns to map to `input` and `output`.
///
/// # Returns
/// A new DataFrame where each record represents a `NeuralAssociations2D` object.
///
/// # Example
///
/// ```
/// use rgwml::df_utils::DataFrame;
/// use rgwml::ai_utils::{NeuralAssociations2DDataFrameConfig, create_neural_associations_2d_df}
///
/// let mut data_frame = Vec::new();
/// let mut record = HashMap::new();
/// record.insert("address".to_string(), Value::String("123 Main St".to_string()));
/// record.insert("name".to_string(), Value::String("John Doe".to_string()));
/// data_frame.push(record);
///
/// let config = NeuralAssociations2DDataFrameConfig {
///     input_column: "address",
///     output_column: "name",
/// };
///
/// let neural_association_df = create_neural_associations_2d_df(data_frame, config);
/// ```
pub fn create_neural_associations_2d_df(
    data_frame: DataFrame,
    config: NeuralAssociations2DDataFrameConfig,
) -> DataFrame {
    let mut neural_association_df = Vec::new();

    for record in data_frame.iter() {
        let input_column = config.input_column.to_string();
        let output_column = config.output_column.to_string();

        let input_value = record
            .get(&input_column)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let output_value = record
            .get(&output_column)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let mut new_row = HashMap::new();
        new_row.insert("input".to_string(), Value::String(input_value));
        new_row.insert("output".to_string(), Value::String(output_value));
        neural_association_df.push(new_row);
    }

    neural_association_df
}
