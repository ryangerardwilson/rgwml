// df_utils.rs
use serde::ser::StdError;
use serde_json::{json, Map, Number, Value};
use std::collections::{HashMap, HashSet};

pub type DataFrame = Vec<HashMap<String, Value>>;

/// Converts a DataFrame into a serde_json Value::Array.
///
/// This function takes a DataFrame as input and converts it into a Value::Array,
/// where each element is a Value::Object constructed from the HashMap entries.
///
/// # Arguments
///
/// * `data_frame` - A DataFrame to convert.
///
/// # Returns
///
/// A serde_json Value::Array containing the converted data.
///
/// # Example
///
/// ```
/// let df = vec![HashMap::from([("key1".to_string(), Value::String("value1".to_string()))])];
/// let value_array = data_frame_to_value_array(df);
/// ```
pub fn dataframe_to_value_array(data_frame: DataFrame) -> Value {
    Value::Array(
        data_frame
            .into_iter()
            .map(|hm| {
                let map: Map<String, Value> = hm.into_iter().collect();
                Value::Object(map)
            })
            .collect(),
    )
}

/// Converts a JSON string into a DataFrame.
///
/// This function takes a JSON string as input and converts it into a DataFrame,
/// which is a vector of HashMaps representing the JSON data structure.
///
/// # Arguments
///
/// * `json_data` - A reference to a JSON string that you want to convert.
///
/// # Returns
///
/// A Result containing the DataFrame if successful, or an error if parsing fails.
///
/// # Example
///
/// ```
/// let json_data = r#"[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]"#;
/// let df = convert_json_string_to_dataframe(json_data).unwrap();
/// ```

pub fn convert_json_string_to_dataframe(json_data: &str) -> Result<DataFrame, Box<dyn StdError>> {
    let data: Value = serde_json::from_str(json_data)?;

    let mut data_frame = DataFrame::new();

    if let Value::Array(objects) = data {
        for object in objects {
            if let Value::Object(map) = object {
                let mut row = HashMap::new();
                for (key, value) in map {
                    row.insert(key, value);
                }
                data_frame.push(row);
            }
        }
    }

    Ok(data_frame)
}

/// Function to get unique values from a DataFrame column.
///
/// This function takes a DataFrame and a column name as input and extracts unique values
/// from the specified column. It returns a DataFrame where each HashMap represents a unique value.
///
/// # Arguments
///
/// * `df` - A reference to the DataFrame from which to extract unique values.
/// * `column_name` - The name of the column from which to extract unique values.
///
/// # Returns
///
/// A DataFrame containing HashMaps, each representing a unique value from the specified column.
///
/// # Example
///
/// ```
/// let df = convert_json_string_to_dataframe(json_data).unwrap();
/// let unique_values_df = get_unique_values(&df, "name");
/// ```
pub fn get_unique_values_dataframe(df: &DataFrame, column_name: &str) -> DataFrame {
    let mut unique_values = HashSet::new();
    let mut unique_values_df = DataFrame::new();

    for row in df {
        if let Some(value) = row.get(column_name) {
            let value_str = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Array(a) => json!(a).to_string(),
                Value::Object(o) => json!(o).to_string(),
                _ => continue, // Skip null and other types
            };

            if unique_values.insert(value_str.clone()) {
                let mut unique_row = HashMap::new();
                unique_row.insert(column_name.to_string(), Value::String(value_str));
                unique_values_df.push(unique_row);
            }
        }
    }

    unique_values_df
}

/// A simple query builder for filtering and limiting data in a DataFrame.
///
/// The `Query` struct allows chaining conditions for filtering and supports limiting the number of records.
///
/// # Examples
///
/// ```
/// use serde_json::{Value, json};
/// use std::collections::HashMap;
/// use std::cmp::Ordering;
///
/// type DataFrame = Vec<HashMap<String, Value>>;
///
/// // Assuming other necessary implementations and functions are present...
///
/// let json_data = r#"[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]"#;
/// let df = convert_json_string_to_dataframe(json_data).unwrap();
///
/// let filtered_df = Query::new(df)
///                     .where_("age", ">", Value::Number(25.into()))
///                     .where_("name", "==", Value::String("Alice".into()))
///                     .limit(10)
///                     .execute();
///
/// assert_eq!(filtered_df.len(), 1); // Assuming this is the expected outcome
/// ```
pub struct Query {
    dataframe: DataFrame,
    conditions: Vec<Box<dyn Fn(&HashMap<String, Value>) -> bool>>,
    limit: Option<usize>,
    selected_columns: Option<Vec<String>>,
}

impl Query {
    /// Creates a new Query with an initial DataFrame.
    ///
    /// # Arguments
    /// - `dataframe`: DataFrame - The dataset to query.
    ///
    /// # Returns
    /// - `Query` - A new Query instance.
    pub fn new(dataframe: DataFrame) -> Self {
        Query {
            dataframe,
            conditions: vec![],
            limit: None,
            selected_columns: None,
        }
    }

    /// Specifies which columns to include in the final result.
    ///
    /// # Arguments
    /// - `columns`: &[&str] - A slice of column names to include in the result.
    ///
    /// # Returns
    /// - `Self` - The Query instance with the selected columns.
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.selected_columns = Some(columns.iter().map(|&col| col.to_string()).collect());
        self
    }

    /// Compares two numbers based on a specified operation.
    ///
    /// # Arguments
    /// - `n1`: &Number - The first number to compare.
    /// - `n2`: &Number - The second number to compare.
    /// - `operation`: &str - The operation (e.g., ">", "<", "==", etc.).
    ///
    /// # Returns
    /// - `bool` - The result of the comparison.
    pub fn compare_numbers(n1: &Number, n2: &Number, operation: &str) -> bool {
        match (n1.as_f64(), n2.as_f64()) {
            (Some(f1), Some(f2)) => match operation {
                ">" => f1 > f2,
                "<" => f1 < f2,
                ">=" => f1 >= f2,
                "<=" => f1 <= f2,
                "==" => f1 == f2,
                _ => false,
            },
            _ => false,
        }
    }

    /// Adds a condition to the query based on a column name, operation, and value.
    ///
    /// # Arguments
    /// - `column_name`: &str - The name of the column to apply the condition.
    /// - `operation`: &str - The operation to use for comparison.
    /// - `value`: Value - The value to compare against.
    ///
    /// # Returns
    /// - `Self` - The Query instance with the new condition added.
    pub fn where_(mut self, column_name: &str, operation: &str, value: Value) -> Self {
        let column_name = column_name.to_string();
        let operation = operation.to_string();
        let condition: Box<dyn Fn(&HashMap<String, Value>) -> bool> = Box::new(move |row| {
            if let Some(row_value) = row.get(&column_name) {
                match (row_value, &value) {
                    (Value::Number(n1), Value::Number(n2)) => {
                        Query::compare_numbers(n1, n2, &operation)
                    }
                    (Value::String(s1), Value::String(s2)) => match operation.as_str() {
                        "==" => s1 == s2,
                        _ => false,
                    },
                    // Add more comparisons as needed
                    _ => false,
                }
            } else {
                false
            }
        });

        self.conditions.push(condition);
        self
    }

    /// Sets a limit for the number of records to return in the query.
    ///
    /// # Arguments
    /// - `limit`: usize - The maximum number of records to return.
    ///
    /// # Returns
    /// - `Self` - The Query instance with the limit set.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Executes the query on the DataFrame.
    ///
    /// # Returns
    /// - `DataFrame` - The resulting DataFrame after applying the conditions and limit.
    pub fn execute(self) -> DataFrame {
        let mut result: DataFrame = self
            .dataframe
            .into_iter()
            .filter(|row| self.conditions.iter().all(|condition| condition(row)))
            .collect();

        if let Some(limit) = self.limit {
            result.truncate(limit);
        }

        if let Some(selected_columns) = self.selected_columns {
            result = result
                .into_iter()
                .map(|mut row| {
                    let mut filtered_row = HashMap::new();
                    for column in &selected_columns {
                        if let Some(value) = row.remove(column) {
                            filtered_row.insert(column.clone(), value);
                        }
                    }
                    filtered_row
                })
                .collect();
        }

        result
    }
}

/// Groups a DataFrame based on a provided key.
///
/// This function groups rows in the given DataFrame based on the value associated
/// with a specified key. It handles string values directly and converts other types
/// of values to strings. If the key is not found in a row, an error message is logged.
///
/// # Arguments
/// - `dataframe`: &DataFrame - The dataset to group.
/// - `key`: &str - The key based on which the grouping is to be done.
///
/// # Returns
/// - HashMap<String, DataFrame> - A HashMap where each key represents a unique value
///   associated with the specified key in the input rows, and each value is a DataFrame
///   containing the grouped rows.
///
/// # Example
/// ```
/// let df = vec![
///     hashmap! {"zone_map" => Value::String("Zone1".to_string()), "data" => Value::Number(1.into())},
///     hashmap! {"zone_map" => Value::String("Zone2".to_string()), "data" => Value::Number(2.into())},
/// ];
/// let grouped = group_dataframe_by(&df, "zone_map");
/// // This will group the DataFrame based on the values of "zone_map".
/// ```
pub fn group_dataframe_by(dataframe: &DataFrame, key: &str) -> HashMap<String, DataFrame> {
    let mut grouped_data: HashMap<String, DataFrame> = HashMap::new();

    for row in dataframe {
        if let Some(value) = row.get(key) {
            let key_value = match value {
                Value::String(s) => s.clone(), // Directly use the string value
                _ => value.to_string(),        // Convert other types to string
            };
            grouped_data
                .entry(key_value)
                .or_insert_with(Vec::new)
                .push(row.clone());
        } else {
            // Handle the case where the key is not found
            eprintln!("Key '{}' not found in row", key);
        }
    }

    grouped_data
}
