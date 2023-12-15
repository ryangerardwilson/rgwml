use futures::future::join_all;
use futures::join;
use fuzzywuzzy::fuzz;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
// use std::collections::HashSet;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Deserialize)]
struct TrainingDataItem {
    input: String,
    output: String,
    destination: String,
}

#[derive(Deserialize)]
struct VpfcGlobalsItem {
    key: String,
    value: String,
    variable_type: String,
}

#[derive(Debug, Serialize, Clone)]
struct VpfcGlobalJson {
    key: String,
    value: String,
    variable_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct ResponseFormat {
    pub amygdala_result: Option<String>,
    pub prefrontal_cortex_result: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct PFCResponseFormat {
    pub hippocampus_result: Option<String>,
    pub ventolateral_pfc_result: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SimilarityResult {
    pub adjusted_similarity: f32,
    pub input: String,
    pub output: String,
}

#[derive(Clone, Debug)]
pub enum Transformer {
    Id(i32),
}

#[derive(Clone, Debug, Deserialize)]
pub struct NeuralAssociations2 {
    pub input: String,
    pub output: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct VpfcGlobals {
    pub key: String,
    pub value: String,
}


impl Transformer {
    pub fn to_string(&self) -> String {
        match self {
            Transformer::Id(id) => id.to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Action {
    Amygdala,
    PrefrontalCortex,
}

pub enum AdjustmentBasis {
    None,
    UserInputLength,
}

pub enum ShowComplications {
    True,
    False,
}

pub enum CallType {
    Linear,
    Parallel,
}

pub enum SplitUpto {
    WordSetLength(usize),
}

enum ConversationScope {
    Scope(usize),
}

enum RoleFilter {
    None,
    User,
    Assistant,
    System,
}


pub async fn parallelize_against_training_data<'a>(
    neural_associations: &'a [NeuralAssociations2],
    text_in_focus: &str,
    // role_filter: RoleFilter,
    // conversation_scope: ConversationScope,
    task_name: &str,
    //adjustment_basis: AdjustmentBasis,
    split_upto: SplitUpto,
    call_type: CallType,
    show_complications: ShowComplications,
) -> Result<String, Box<dyn Error>> {
    fn remove_tags(s: &str) -> String {
        s.replace("<mv>", "")
            .replace("</mv>", "")
            .replace("<imv>", "")
            .replace("</imv>", "")
            .replace("{}", "")
    }

    async fn process_item(
        items_to_process: String,
        data_arc: Arc<&[NeuralAssociations2]>,
        // data: Arc<Vec<CsvRow>>,
        user_input_length: usize,
        task_name: &str,
        //adjustment_basis: &AdjustmentBasis,
        show_complications: &ShowComplications,
    ) -> Option<SimilarityResult> {
        let mut all_futures: Vec<Pin<Box<dyn Future<Output = SimilarityResult>>>> = Vec::new();

        for row in data_arc.as_ref().iter() {
            let clean_row_input = remove_tags(&row.input).to_lowercase();
            let input_word_count = row.input.split_whitespace().count() as i32;
            let items_to_process_lower = items_to_process.to_lowercase();

            let similarity = fuzz::ratio(&items_to_process_lower, &clean_row_input);

            // let similarity = fuzz::ratio(&items_to_process, &row.input);
            let word_count_diff = (user_input_length as i32 - input_word_count as i32).abs();

            /*
            let adjusted_similarity = match adjustment_basis {
                AdjustmentBasis::None => similarity as f32,
                AdjustmentBasis::UserInputLength => {
                    similarity as f32 - (similarity as f32 * 0.25 * word_count_diff as f32)
                }
            };
            */

            // let adjusted_similarity = similarity;

            if let ShowComplications::True = show_complications {
                println!(
                    "Comparing ({}) '{}' with '{}' - Similarity: {}",
                    &task_name, &items_to_process, &row.input, similarity
                );
            }

            all_futures.push(Box::pin(async move {
                SimilarityResult {
                    // adjusted_similarity: similarity as f32 - (similarity as f32 * 0.10 * word_count_diff as f32),
                    adjusted_similarity: similarity as f32,
                    input: row.input.to_string(),
                    output: row.output.to_string(),
                    //input: row.input.clone(),
                    //output: row.output.clone(),
                }
            }));
        }

        let results = join_all(all_futures).await;

        let best_result = results.clone().into_iter().max_by(|a, b| {
            a.adjusted_similarity
                .partial_cmp(&b.adjusted_similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        //let top_5_results = &results.into_iter().take(5).collect::<Vec<_>>();

        //dbg!(top_5_results);

        best_result
    }

    async fn process_message_concurrently(
        message: &str,
        data_arc: Arc<&[NeuralAssociations2]>,
        //data_arc: Arc<Vec<NeuralAssociations>>,
        split_upto: &SplitUpto,
        task_name: &str,
        call_type: &CallType,
        // // adjustment_basis: &AdjustmentBasis,
        show_complications: &ShowComplications,
        decrement_coefficient: f64,
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

            // dbg!(&words, &data_arc);
            //dbg!(&words.len());
            //dbg!(&data_arc.len());

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
                        //&adjustment_basis,
                        &show_complications,
                    )
                })
                .collect();

            let results = join_all(word_and_phrase_futures).await;

            // dbg!(&results);
            match call_type {
                CallType::Parallel => {
                    let mut max_similarity_per_output: HashMap<String, SimilarityResult> =
                        HashMap::new();


                    dbg!(&results);
                    for result_option in results {
                        if let Some(result) = result_option {
                            let output = result.output.clone(); // Clone the output to use as a key

                            // Every result is now considered for insertion or update, without checking similarity
                            if let Some(entry) = max_similarity_per_output.get_mut(&output) {
                                // Update the entry if the current result has a higher similarity
                                if result.adjusted_similarity > entry.adjusted_similarity {
                                    *entry = result.clone(); // Clone the result for insertion
                                }
                            } else {
                                // Insert the result if no entry exists for this output
                                max_similarity_per_output.insert(output, result.clone());
                            }
                        }
                    }

                    // Extract the values (SimilarityResults) from the map and collect them into a Vec
                    let filtered_results: Vec<_> =
                        max_similarity_per_output.values().cloned().collect();

                    dbg!(&filtered_results);

                    // Optionally sort and take the top 3 results based on adjusted_similarity
                    let mut sorted_filtered_results = filtered_results;
                    sorted_filtered_results.sort_by(|a, b| {
                        b.adjusted_similarity
                            .partial_cmp(&a.adjusted_similarity)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });

                    let top_results = sorted_filtered_results
                        .into_iter()
                        .take(7)
                        //.map(|mut result| {
                        //    result.adjusted_similarity *= 1.0f32 - decrement_coefficient as f32;
                        //     result
                        //})
                        .collect::<Vec<_>>();

                    dbg!(&top_results);

                    if !top_results.is_empty() {
                        // Serialize the top results into a JSON string
                        let json = serde_json::to_string(&top_results)?;
                        Ok(json)
                    } else {
                        Err("No suitable results found for Parallel call type".into())
                    }
                }

                CallType::Linear => {
                    let best_output = results
                        .into_iter()
                        .filter_map(|result| result) // filter out None results
                        .max_by(|a, b| {
                            a.adjusted_similarity
                                .partial_cmp(&b.adjusted_similarity)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });

                    if let Some(best_similarity_result) = best_output {
                        let mut best_similarity_result = best_similarity_result;

                        best_similarity_result.adjusted_similarity *=
                            1.0f32 - decrement_coefficient as f32;
                        // Wrap the best result in an array
                        let wrapped_result = vec![best_similarity_result];
                        // Serialize the wrapped result into a JSON string
                        let json = serde_json::to_string(&wrapped_result)?;
                        Ok(json)
                    } else {
                        // Handle the case where no valid results were found
                        Err("No user message found in the conversation".into())
                    }
                }
            }
        }

    let data_arc = Arc::new(neural_associations);


    let mut futures = Vec::new();

    let mut decrement = 0.0;

    //for message in conversation_messages.iter() {

    let data_arc_clone = Arc::clone(&data_arc);
    let future = process_message_concurrently(
        text_in_focus,
        data_arc_clone,
        &split_upto,
        &task_name,
        &call_type,
        // &adjustment_basis,
        &show_complications,
        decrement,
    );
    futures.push(future);

    let results = join_all(futures).await;

    //let mut most_similar_result = None;
    let mut most_similar_json = String::new();
    let mut max_similarity = 0.0;

    for result in results.iter() {
        if let Ok(json_string) = result {
            // Deserialize the JSON string into a Vec<SimilarityResult>
            if let Ok(parsed_results) = serde_json::from_str::<Vec<SimilarityResult>>(json_string) {
                // Access the first (and only) SimilarityResult in the vector
                if let Some(similarity_result) = parsed_results.first() {
                    // Check if this result has a higher similarity than the current max
                    if similarity_result.adjusted_similarity > max_similarity {
                        max_similarity = similarity_result.adjusted_similarity;
                        most_similar_json = json_string.clone();
                    }
                }
            }
        }
    }

    // After the loop, check if a most similar JSON string was found
    if !most_similar_json.is_empty() {
        // dbg!(&most_similar_json);
        // Return the JSON string of the most similar result
        Ok(most_similar_json)
    } else {
        // Handle the case where no similar results were found
        // Err("No similar results found".into())
        Ok("{}".to_string())
    }
}



