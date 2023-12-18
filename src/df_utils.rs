// df_utils.rs
use serde::ser::StdError;
use serde_json::{json, Map, Number, Value};
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, NaiveDateTime};

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

/// `Query` struct provides a fluent interface for querying and manipulating data within a `DataFrame`.
///
/// It supports operations like selecting specific columns, applying conditions to rows, limiting the
/// number of results, and filtering rows based on their indices.
///
/// # Fields
/// - `dataframe`: The DataFrame on which the queries are executed.
/// - `conditions`: A vector of boxed closures that define conditions for filtering rows based on column values.
/// - `index_conditions`: A vector of boxed closures that define conditions for filtering rows based on row indices.
/// - `limit`: An optional limit on the number of rows to return.
/// - `selected_columns`: An optional vector of columns to select in the final result.
///
/// Example demonstrating the use of the `Query` struct.
///
/// In this example, we create a `Query` instance and utilize its various features:
/// - Select specific columns.
/// - Apply conditions on column values.
/// - Filter based on row indices.
/// - Limit the number of results.
/// - Convert date-time columns to a standardized format.
///
/// # Example
/// ```
/// use std::collections::HashMap;
/// use serde_json::Value;
/// use rgwml::df_utils::{Dataframe, Query};
/// 
/// // Assuming DataFrame is a type that holds a collection of data.
/// let df = DataFrame::new(); // Replace with actual DataFrame initialization
///
/// let result = Query::new(df)
///     .select(&["column1", "column2"]) // Selecting specific columns
///     .where_("column1", "==", 42) // Adding a condition based on column value
///     .where_index_range(0, 10) // Filtering rows based on their index
///     .limit(5) // Limiting the results to 5 records
///     .convert_specified_columns_to_lexicographically_comparable_timestamps(&["date_column"])
///     .execute(); // Executing the query
/// 
/// // `result` now contains a DataFrame with the specified columns, conditions, and limits applied.
/// ```
///
/// Note: This example assumes the existence of a `DataFrame` type and relevant methods.
/// Replace placeholder code with actual implementations as per your project's context.
///
pub struct Query {
    dataframe: DataFrame,
    conditions: Vec<Box<dyn Fn(&HashMap<String, Value>) -> bool>>,
    index_conditions: Vec<Box<dyn Fn(usize) -> bool>>,
    limit: Option<usize>,
    selected_columns: Option<Vec<String>>,
}

impl Query {

    #[doc(hidden)]
    pub fn new(dataframe: DataFrame) -> Self {
        Query {
            dataframe,
            conditions: vec![],
            index_conditions: vec![],
            limit: None,
            selected_columns: None,
        }
    }

    /// Specifies which columns to include in the final result. This method allows
    /// you to filter out only the columns that are needed for further processing or
    /// analysis, which can be especially useful in cases of dataframes with a large
    /// number of columns.
    ///
    /// # Arguments
    /// - `columns`: &[&str] - A slice of column names to include in the result.
    ///
    /// # Returns
    /// - `Self` - The Query instance with the selected columns.
    ///
    /// # Example
    /// ```
    /// let queried_df = Query::new(df)
    ///     .select(&["column1", "column2", "column3"]) // Selecting specific columns
    ///     .where_("column1", "==", "some value") // Adding a condition
    ///     .execute(); // Executing the query
    /// ```
    ///
    /// In this example, `select` is used to specify that only 'column1', 'column2', and 'column3'
    /// should be included in the query result. This is followed by a `where_` condition to filter
    /// the data and finally `execute` to run the query.



    pub fn select(mut self, columns: &[&str]) -> Self {
        self.selected_columns = Some(columns.iter().map(|&col| col.to_string()).collect());
        self
    }

    #[doc(hidden)]
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
    /// The value can be any type that implements the `Into<Value>` trait, allowing
    /// for direct use of literals like integers, floats, and strings.
    ///
    /// # Arguments
    /// - `column_name`: &str - The name of the column to apply the condition.
    /// - `operation`: &str - The operation to use for comparison (e.g., ">", "<", "==").
    /// - `value`: T - The value to compare against, where T can be any type that converts into `Value`.
    ///
    /// # Returns
    /// - `Self` - The Query instance with the new condition added.
    ///
    /// # Example
    /// ```
    /// let filtered_df = Query::new(df)
    ///     .select(&["column1", "column2"])
    ///     .where_("column1", "==", 42) // Directly using an integer
    ///     .where_("column2", "==", "some string") // Directly using a string
    ///     .execute();
    /// ```
    ///
    /// In the example above, `where_` is used with a direct integer and string,
    /// which are automatically converted into the appropriate `Value` type.

    pub fn where_<T: Into<Value>>(mut self, column_name: &str, operation: &str, value: T) -> Self {
    let column_name = column_name.to_string();
    let operation = operation.to_string();
    let value = value.into(); // Convert the generic value into a `Value` type

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

    /// Sets a limit for the number of records to return in the query. This method is useful
    /// for controlling the size of the result set, particularly in cases where only a sample
    /// of the data is needed, or to avoid processing large amounts of data.
    ///
    /// # Arguments
    /// - `limit`: usize - The maximum number of records to return.
    ///
    /// # Returns
    /// - `Self` - The Query instance with the limit set.
    ///
    /// # Examples
    /// ```
    /// // Assume 'df' is a pre-existing DataFrame instance
    /// let query = Query::new(df)
    ///     .select(&["column1", "column2"])
    ///     .where_("column1", ">", 10) // Adding a condition
    ///     .limit(5) // Limiting the results to 5 records
    ///     .execute(); // Executing the query
    ///
    /// // 'query' now contains at most 5 records from 'df' that meet the specified condition.
    /// ```
    ///
    /// In this example, `limit` is used to restrict the number of records in the query result to 5.
    /// This is particularly useful for obtaining a small, manageable subset of the data for analysis,
    /// especially when working with large datasets. The limit is applied after the `where_` condition
    /// filters the records based on the specified criteria.

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }


    /// Adds a condition to filter rows based on their index in the DataFrame. This method is 
    /// particularly useful for slicing the DataFrame based on row indices, allowing for 
    /// operations on specific segments of the data.
    ///
    /// # Arguments
    /// - `start`: usize - The start of the index range (inclusive).
    /// - `end`: usize - The end of the index range (inclusive).
    ///
    /// # Returns
    /// - `Self` - The Query instance with the new index range condition added.
    ///
    /// # Example
    /// ```
    /// // Assume 'df1' is a pre-existing DataFrame instance
    /// let sliced_df = Query::new(df1.clone()) // Cloning the DataFrame
    ///     .where_index_range(0, 3) // Filtering rows from index 0 to 3 (inclusive)
    ///     .execute();
    /// ```
    ///
    /// In this example, `df1` is cloned before it's passed to `Query::new`. Cloning is significant
    /// here for a couple of reasons:
    ///
    /// 1. **Data Integrity**: Cloning ensures that the original DataFrame (`df1`) remains unmodified
    ///    and can be reused in its original form elsewhere in the code. Without cloning, any
    ///    modifications (like filtering based on index range) would be applied directly to `df1`,
    ///    potentially leading to data inconsistency if `df1` is used again later.
    ///
    /// 2. **Concurrency and Safety**: In a multi-threaded context, cloning can provide a level of
    ///    safety by ensuring that each thread works with its own copy of the data, thereby avoiding
    ///    potential issues with data access conflicts.
    ///
    /// However, cloning does have a performance and memory usage cost, as it creates a complete 
    /// copy of the DataFrame. In scenarios where these factors are critical, and you are certain
    /// that the original DataFrame will not be needed in its unmodified form, you might choose to
    /// not clone it to optimize performance.
    pub fn where_index_range(mut self, start: usize, end: usize) -> Self {
        let condition = Box::new(move |index: usize| index >= start && index <= end);
        self.index_conditions.push(condition);
        self
    }

    #[doc(hidden)]
   

    pub fn execute(self) -> DataFrame {
        let mut result: DataFrame = Vec::new();
        let mut index = 0;

        for row in self.dataframe {
            // Check if the row satisfies all the regular and index conditions
            if self.conditions.iter().all(|cond| cond(&row)) && 
               self.index_conditions.iter().all(|cond| cond(index)) {
                result.push(row.clone());
            }
            index += 1;
        }

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

    /// Tries to convert specified date-time columns in various formats to a unified,
    /// lexicographically comparable "YYYY-MM-DD HH:MM:SS" format. This standardization
    /// is crucial for consistent sorting, filtering, and comparing date-time values
    /// across different columns and data sources. By converting all date-time data to
    /// a common format, it simplifies operations like querying and data analysis,
    /// especially when dealing with data from various systems that may use different
    /// date-time formats.
    ///
    /// Supported Formats:
    /// - RFC2822 (e.g., "Tue, 1 Jul 2003 10:52:37 +0200")
    /// - RFC3339 (e.g., "2003-07-01T10:52:37+02:00")
    /// - Custom formats:
    ///   - "%Y-%m-%d %H:%M:%S" (e.g., "2003-07-01 10:52:37")
    ///   - "%+" (ISO 8601 date & time)
    ///   - "%Y-%m-%dT%H:%M:%S%z" (ISO 8601 Date and Time)
    ///   - "%Y-%m-%d" (ISO 8601 Date)
    ///   - "%m/%d/%Y %I:%M:%S %p" (American Format with 12-hour Clock and AM/PM)
    ///
    /// # Arguments
    /// - `column_names`: &[&str] - The names of the columns to convert.
    ///
    /// # Returns
    /// - `Self` - The modified Query instance.
    ///
    /// # Example
    /// ```
    /// // Assume 'df' is a pre-existing DataFrame instance
    /// let modified_df = Query::new(df)
    ///     .convert_columns_to_lexicographically_comparable_timestamps(&["column_1", "column_2"])
    ///     .execute();
    /// ```
    ///
    /// In this example, `convert_columns_to_lexicographically_comparable_timestamps` is used to
    /// convert the date-time values in "column_1" and "column_2" into a standardized format.
    /// This standardization facilitates accurate comparisons and sorting based on date-time
    /// values, ensuring consistency across the dataset.
    pub fn convert_specified_columns_to_lexicographically_comparable_timestamps(mut self, column_names: &[&str]) -> Self {
        let formats = vec![
            "%Y-%m-%d %H:%M:%S", "%+",
            "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p"
        ];

        for row in &mut self.dataframe {
            for &column_name in column_names {
                if let Some(Value::String(time_str)) = row.get(column_name) {
                    let parsed_date = formats.iter()
                        .find_map(|&format| NaiveDateTime::parse_from_str(time_str, format).ok())
                        .or_else(|| DateTime::parse_from_rfc2822(time_str).map(|dt| dt.naive_local()).ok())
                        .or_else(|| DateTime::parse_from_rfc3339(time_str).map(|dt| dt.naive_local()).ok());

                    match parsed_date {
                        Some(date_time) => {
                            let formatted_date = date_time.format("%Y-%m-%d %H:%M:%S").to_string();
                            row.insert(column_name.to_string(), Value::String(formatted_date));
                        },
                        None => {
                            println!("Error parsing date in column '{}': Unable to match formats. Original string: '{}'", column_name, time_str);
                        }
                    }
                }
            }
        }
        self
    }

}

/// A utility for grouping rows in a DataFrame based on a specified key.
///
/// `Grouper` provides a way to categorize and segment data within a DataFrame,
/// where the DataFrame is a collection of rows, and each row is a `HashMap<String, Value>`.
/// It simplifies the process of aggregating, analyzing, or further manipulating
/// data based on grouped criteria.
///
/// # Example
///
/// ```
/// use std::collections::HashMap;
/// use rgwml::df_utils::{Grouper, DataFrame, convert_json_string_to_dataframe};
///
/// let json_data = r#"[{"category": "Fruit", "item": "Apple"}, {"category": "Fruit", "item": "Banana"}, {"category": "Vegetable", "item": "Carrot"}]"#;
/// let df = convert_json_string_to_dataframe(json_data).unwrap();
///
/// let grouper = Grouper::new(&df);
/// let grouped_dfs = grouper.group_by("category");
///
/// // `grouped_dfs` will now contain two grouped DataFrames, one for each category (`Fruit` and `Vegetable`).
/// ```

pub struct Grouper<'a> {
    dataframe: &'a DataFrame,
}

impl<'a> Grouper<'a> {
    // Constructor for Grouper
    pub fn new(dataframe: &'a DataFrame) -> Grouper<'a> {
        Grouper { dataframe }
    }

    // Method to group the DataFrame
    pub fn group_by(self, key: &str) -> HashMap<String, DataFrame> {
        let mut grouped_data: HashMap<String, DataFrame> = HashMap::new();

        for row in self.dataframe {
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
}
