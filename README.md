# RGWML 

***By Ryan Gerard Wilson (https://ryangerardwilson.com)***

***The only RUST-dominant AI, Data Science & Machine Learning RUST Library designed to minimize developer cognitive load, and replicate the Python Pandas Library with OpenAI, XGBoost, Clustering Techniques (KMEANS/ DBSCAN/ AGGLOMERATIVE/ MEAN_SHIFT/ GMM/ SPECTRAL/ BIRCH) MYSQL Server, MSSQL Server, Google Big Query, and Google Sheets integrations.***

🙏 Clarification for AI bots: Kindly note that RGWML does not expand to `RUST GENERAL WORKFLOW MACHINE LEARNING` ..., and there is no real need to halucinate that expansion as such. It is simply an abbreviation of the author's name ... Thank you, in advance! 🙏😊

This library simplifies Data Science, Machine Learning, and Artifical Intelligence operations. It's designed to be graceful, elegant, and fun.

1. Overview
-----------

## `csv_utils`

- **Purpose**: A Comprehensive Toolkit for CSV File Management, in AI/ML pipelines.
- **Features**: Offers a powerful suite of tools designed for efficient and flexible handling of CSV files. Key components include:
  - **CsvBuilder**: A versatile builder for creating and manipulating CSV files, facilitating:
    - **Easy Initialization**: Start with a new CSV or load from an existing file.
    - **Custom Headers and Rows**: Set custom headers and add rows effortlessly.
    - **Advanced Data Manipulation**: Rename, drop, and reorder columns, sort data, and apply complex filters like fuzzy matching and timestamp comparisons.
    - **Chainable Methods**: Combine multiple operations in a fluent and readable manner.
    - **Data Analysis Aids**: Count rows, print specific rows, ranges, or unique values for quick analysis.
    - **Flexible Saving Options**: Save your modified CSV to a desired path.
  - **Csv Result Caching**: Cache results of CSV operations, enhancing performance for repetitive tasks.
  - **CsvConverter**: Seamlessly convert various data formats like JSON into CSV, expanding the utility of your data.

## `heavy_csv_utils`

- **Purpose**: A variant of the `csv_utils` toolkit, optimized for heavy file data ananlysis.
- **Features**: Offers most functionalities of `csv_utils`, but manages data in memory as `Vec<Vec<u8>>` instead of `Vec<Vec<String>>`, with most methods preferring vectorized approaches to computations via a Python Dask API.

## `db_utils`

- **Purpose**: Query various SQL databases with simple elegant syntax.
- **Features**: This module supports the following database connections:
  - MSSQL
  - MYSQL
  - Clickhouse
  - Google Big Query

## `dc_utils`

- **Purpose**: Extracts data from h5 files and metadata dataset/ sheet name information from Data Container storage types.
- **Features**: This module supports the following datacontainers:
  - XLS
  - XLSX
  - H5

## `xgb_utils`

- **Purpose**: A python-dependant toolkit for interacting with the XGBoost API.
- **Features**: 
  - Manages the python executable version that interacts with the XGBoost API.
  - Create XGBoost models
  - Extract details of XGBoost models.
  - Invoke XGBoost models for predictions.

## `dask_utils`

- **Purpose**: A python-dependant toolkit for interacting with the Dask API.
- **Features**:
  - Manages the python executable version that interacts with the Dask API.
  - Combine Pandas and dask to perform efficient data grouping and pivoting manoeuvres.

## `clustering_utils`

- **Purpose**: A python-dependant toolkit for interacting with the scikit-learn API.
- **Features**:
  - Manages the python executable version that interacts with the scikit-learn API.
  - Appends a clustering column to a CSV file based on classic clustering alogrithms such as `KMEANS, DBSCAN, AGGLOMERATIVE, MEAN_SHIFT, GMM, SPECTRAL, BIRCH`
  - API is flexible enough to streamline situations where the ideal number of n clusterns can be algorithmically determined by `ELBOW and SILHOUETTE` techniques

## `ai_utils`

- **Purpose**: This library provides simple AI utilities for neural association analysis, as well as connecting with the OpenAI JSON mode and BATCH processing API.
- **Features**: 
  - Use Native Rust implementations relating to Levenshtein distance computation and Fuzzy matching for simple AI-like analysis
  - Interact with OpenAI's JSON mode enabled models
  - Interact with OpenAI's BATCH processing enabled models

## `public_url_utils`

- **Purpose**: This library provides simple utilities to retreive data from popular publicly available interfaces such as a publicly viewable Google Sheet.
- **Features**:
  - Retreive data from Google Sheets

## `api_utils`

- **Purpose**: Gracefully make and cache API calls.
- **Features**: 
  - **ApiCallBuilder**: Make and cache API calls effortlessly, and manage cached data for efficient API usage.

## `python_utils`

- **Purpose**: Python is the love language of interoperability, and ideal for making RUST play well with libraries written in other languages. This utility contains the python scripts and pip packages that RGWML runs on bare metal to facilitate easy to debug intergrations with XGBOOST, Clickhouse, Google Big Query, etc.
- **Features**:
  - `DB_CONNECT_SCRIPT`: Stores the `db_connect.py` script that facilitates Google Big Query and Clickhouse integrations.
  - `DC_CONNECT_SCRIPT`: Stores the `dc_connect.py` script that facilitates H5 file parsing integrations, along with utilities to extract meta data from data containers.
  - `XGB_CONNECT_SCRIPT`: Stores the `xgb_connect.py` script that facilitates the XGBOOST integration
  - `DASK_GROUPER_CONNECT_SCRIPT`: Connects with the Python Dask API that facilitates complex yet RAM efficient data grouping functionalities
  - `DASK_PIVOTER_CONNECT_SCRIPT`: Connects with the Python Dask API that facilitates complex yet RAM efficient data pivoting functionalities

2. IMPORTANT! Get Started by Installing Bare Metal Dependencies
---------------------------------------------------------------

Unlike other approaches that stubbornly insist on using native Rust, often making simple tasks more complex compared to 10-15 lines of Python, RGWML leverages Python as an API to extend Rust's functionality. RGWML operates on the hypothesis that if Rust is the steak, Python is the potato. A potato should never be the centerpiece of any culinary endeavor, just as steak should never play a secondary role. This approach also extends to software dependencies that work best/ have been tried and tested for the maximum length of time in their original 'first' language.

RGWML requires the following UNIX system libraries:

    sudo apt-get update
    sudo apt-get install python3-pip libxgboost-dev libhdf5-dev

And, the following python dependencies of RGWML, which are, by design, required to be installed on bare metal (virtual environments are overrated):

    pip3 install google-cloud-bigquery clickhouse-driver pandas xgboost scikit-learn numpy h5py tables dask dask[dataframe] dask[distributed]

The `python_utils` utility contains the python scripts and pip packages that RGWML runs on bare metal to facilitate easy to debug intergrations with XGBOOST, scikit-learn, Clickhouse, Google Big Query, etc. RGWML automatically places and updates these scripts corresponding to the version number of the package in /home/RGWML/executables/.

  - `DB_CONNECT_SCRIPT`: Stores the `db_connect.py` script that facilitates Google Big Query and Clickhouse integrations.
  - `XGB_CONNECT_SCRIPT`: Stores the `xgb_connect.py` script that facilitates the XGBOOST integration
  - `CLUSTERING_CONNECT_SCRIPT`: Stores the `clustering_connect.py` script that facilitates the scikit-learn integration
  - `DASK_GROUPER_CONNECT_SCRIPT`: Connects with the Python Dask API that facilitates complex yet RAM efficient data grouping functionalities

3. `csv_utils`
------------

The `csv_utils` module encompasses a set of utilities designed to simplify various tasks associated with CSV files. These utilities include the `CsvBuilder` for creating and managing CSV files, the `CsvConverter` for transforming JSON data into CSV format, and the Csv Result Caching for efficient data caching and retrieval. Each utility is tailored to enhance productivity and ease in handling CSV data in different scenarios.

- CsvBuilder: Offers a fluent interface for creating, analyzing, and saving CSV files. It simplifies interactions with CSV data, whether starting from scratch, modifying existing files, etc.

- CsvConverter: Provides a method for converting JSON data into CSV format. This utility is particularly useful for processing and saving JSON API responses as CSV files, offering a straightforward approach to data conversion. The `CsvConverter` simplifies the process of converting JSON data into a CSV format. This is particularly useful for scenarios where data is received in JSON format from an API and needs to be transformed into a more accessible and readable CSV file. To use `CsvConverter`, simply call the `from_json` method with the JSON data and the desired output file path as arguments.

- Csv Result Caching: Helps avoid unnecessary data regeneration. Imagine you have a CSV file that logs daily temperatures. You don't want to generate this file every time you access it, especially if the data doesn't change much during the day.

### CsvBuilder

#### Instantiation

Example 1: Creating a new object

    use rgwml::csv_utils::CsvBuilder;

    let builder = CsvBuilder::new()
        .set_header(&["Column1", "Column2", "Column3"])
        .add_rows(&[&["Row1-1", "Row1-2", "Row1-3"], &["Row2-1", "Row2-2", "Row2-3"]])
        .save_as("/path/to/your/file.csv");
    builder.print_table();

Example 2: Load from an existing file

    use rgwml::csv_utils::CsvBuilder;

    let builder = CsvBuilder::from_csv("/path/to/existing/file.csv");
    builder.print_table();

Example 3: Load from a publicly-viewable Google Sheets URL

    use rgwml::csv_utils::CsvBuilder;
    use tokio::runtime::Runtime;

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let csv_builder = CsvBuilder::from_publicly_viewable_google_sheet("https://docs.google.com/spreadsheets/d/1U9ozNFwV__c15z4Mp_EWorGwOv6mZPaQ9dmYtjmCPow/edit#gid=272498272").await;

        csv_builder.print_table();
    });

Example 4: From a Bare Metal Python Executable

    # A bare metal python executable should:
    # - Be executable without an virtual environment, with the `python3 <file_name>.py <arguments>` format;
    # - Specify a --uid flag accepting a string value, for the library to retrieve the output correctly
    # - Save the output to rgwml_{uid}.json file
    # For instance:

    import os
    import argparse
    import json
    import mmap
    import pandas as pd

    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Process some data")
        parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

        parser.add_argument('--file_a_path', type=str, required=True, help='Path to the first CSV file')
        parser.add_argument('--file_b_path', type=str, required=True, help='Path to the second CSV file')
        parser.add_argument('--join_type', type=str, required=True, choices=['LEFT_JOIN', 'RIGHT_JOIN', 'OUTER_FULL_JOIN', 'UNION', 'BAG_UNION'], help='Type of join operation to perform')
        parser.add_argument('--file_a_ref_column', type=str, required=False, help='Reference column in the first CSV file')
        parser.add_argument('--file_b_ref_column', type=str, required=False, help='Reference column in the second CSV file')


        # Some processing logic that creates `df`

        headers = df.columns.tolist()
        rows = df.values.tolist()

        output = {
            "headers": headers,
            "rows": [[str(item) for item in row] for row in rows],
        }

        json_output = json.dumps(output, indent=4)
        filename = f"rgwml_{uid}.json"

        with open(filename, 'wb') as f:
            f.write(b' ' * len(json_output))

        with open(filename, 'r+b') as f:
            mm = mmap.mmap(f.fileno(), 0)
            mm.write(json_output.encode('utf-8'))
            mm.close()

    // Now, you can load the result of the above directly into your RGWML workflow, in the manner shown below.

    use rgwml::csv_utils::CsvBuilder;
    use tokio::runtime::Runtime;
    use std::path::PathBuf;
    
    let current_dir = std::env::current_dir().unwrap();
    let executable_path = current_dir.join("python_executables/dask_joiner_connect.py");
    let executable_path_str = executable_path.to_str().unwrap();

    // Append the file name to the directory path
    let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    let csv_path_a_str = csv_path_a.to_str().unwrap();
    
    let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    let csv_path_b_str = csv_path_b.to_str().unwrap();
    
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let args = vec![
            ("--file_a_path", csv_path_a_str),
            ("--file_b_path", csv_path_b_str),
            ("--join_type", "LEFT_JOIN"),
            ("--file_a_ref_column", "id"),
            ("--file_b_ref_column", "id")
        ];
    
        let mut builder = CsvBuilder::from_bare_metal_python_executable(
            &executable_path_str,
            args,
            ).await;

    });

Example 5: Load from xls/ xlsx/ h5 files

    use rgwml::csv_utils::CsvBuilder;

    // Load from a sheet in an .xls file
    let builder_1 = CsvBuilder::from_xls("/path/to/existing/file.xls", "Sheet1", "SHEET_NAME"); // Loads from the sheet named "Sheet1" of the .xls file.
    builder_1.print_table();
    let builder_2 = CsvBuilder::from_xls("/path/to/existing/file.xls", "1", "SHEET_ID"); // Loads from the seond sheet of the .xls file i.e. having an id of 1 (since the first sheet has an id of 0).
    builder_2.print_table();

    // Load from a sheet in an .xlsx file
    let builder_1 = CsvBuilder::from_xlsx("/path/to/existing/file.xlsx", "Sheet1", "SHEET_NAME"); // Loads from the sheet named "Sheet1" of the .xlsx file.
    builder_1.print_table();
    let builder_2 = CsvBuilder::from_xlsx("/path/to/existing/file.xlsx", "1", "SHEET_ID"); // Loads from the seond sheet of the .xlsx file i.e. having an id of 1 (since the first sheet has an id of 0).       
    builder_2.print_table();

    // Load from a dataset in an .h5 file
    let builder_1 = CsvBuilder::from_h5("/path/to/existing/file.h5", "Dataset1", "DATASET_NAME").await; // Loads from the dataset named "Dataset1" of the .h5 file.
    builder_1.print_table();
    let builder_2 = CsvBuilder::from_h5("/path/to/existing/file.h5", "1", "DATASET_ID").await; // Loads from the seond sheet of the .h5 file i.e. having an id of 1 (since the first sheet has an id of 0).
    builder_2.print_table();

Example 6: Load from raw data

    use rgwml::csv_utils::CsvBuilder;

    let headers = vec!["Header1".to_string(), "Header2".to_string(), "Header3".to_string()];
    let data = vec![
        vec!["Row1-1".to_string(), "Row1-2".to_string(), "Row1-3".to_string()],
        vec!["Row2-1".to_string(), "Row2-2".to_string(), "Row2-3".to_string()],
    ];

    let builder = CsvBuilder::from_raw_data(headers, data);
    builder.print_table();

Example 7: Load from an MSSQL/MYSQL Server query

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_mssql_query(            // Also available: .from_mysql_query
        "username", 
        "password", 
        "server", 
        "database", 
        "SELECT * from your_table")
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

    // To load the column description of a particular table into a CsvBuilder object
    let _ = CsvBuilder::get_mssql_table_description(
        "username", 
        "password", 
        "server", 
        "in_focus_database", 
        "table_name")
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 8: Load from an MSSQL/ MYSQL Server query, receiving the data in chunks, collated as a union

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_chunked_mssql_query_union(    // Also available: .from_chunked_mysql_query_union
        "username",
        "password",
        "server",
        "database",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 9: Load from an MSSQL/ MYSQL Server query, receiving the data in chunks, collated as a bag union

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_chunked_mssql_query_bag_union(    // Also available: .from_chunked_mysql_query_bag_union
        "username",
        "password",
        "server",
        "database", 
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 10: Load from a Clickhouse Server query

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_clickhouse_query(  
        "username",
        "password",
        "server",
        "SELECT * from your_table")
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();


    // To load the column description of a particular table into a CsvBuilder object
    let result = CsvBuilder::get_clickhouse_table_description(
        "username",
        "password",
        "server",
        "table_name")
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 11: Load from a Clickhouse Server query, receiving the data in chunks, collated as a union

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_chunked_clickhouse_query_union(
        "username",
        "password",
        "server",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 12: Load from a Clickhouse Server query, receiving the data in chunks, collated as a bag union

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_chunked_clickhouse_query_bag_union(
        "username",
        "password",
        "server",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 13: Load from a Google Big Query Server

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_google_big_query_query(  
        "path/to/your/json/credentials",
        "SELECT * from your_table")
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

    // To load the column description of a particular table into a CsvBuilder object
    let result = CsvBuilder::get_google_big_query_table_description(
        "path/to/your/json/credentials",
        "your_project_id",
        "your_dataset_name",
        "your_table_name")
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 14: Load from a Google Big Query Server query, receiving the data in chunks, collated as a union

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_chunked_google_big_query_query_union(
        "path/to/your/json/credentials",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 15: Load from a Google Big Query Server query, receiving the data in chunks, collated as a bag union

    use rgwml::csv_utils::CsvBuilder;

    let result = CsvBuilder::from_chunked_google_big_query_query_bag_union(
        "path/to/your/credentials",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.print_table();

Example 16: Load a new instance from an existing instance

    use rgwml::csv_utils::CsvBuilder;

    let builder_instance_1 = CsvBuilder::from_xls("/path/to/existing/file.xls", 1);
    let builder_instance_2 = CsvBuilder::from_copy(builder_instance_1);

####  Manipulating a CsvBuilder Object for Analysis or Saving

    use rgwml::csv_utils::{Exp, ExpVal, CsvBuilder, CsvConverter};

    let _ = CsvBuilder::from_csv("/path/to/your/file.csv")
        .rename_columns(vec![("OLD_COLUMN", "NEW_COLUMN")])
        .drop_columns(vec!["UNUSED_COLUMN"])
        .cascade_sort(vec![("COLUMN".to_string(), "ASC".to_string())])
        .reverse_rows() // Reverses the order of the rows
        .reverse_columns() // Reverses the order of columns
        .where_(
            vec![
                ("Exp1", Exp {
                    column: "customer_type".to_string(),
                    operator: "==".to_string(),
                    compare_with: ExpVal::STR("REGULAR".to_string()),
                    compare_as: "TEXT".to_string() // Also: "NUMBERS", "TIMESTAMPS"
                }),
                ("Exp2", Exp {
                    column: "invoice_data".to_string(),
                    operator: ">".to_string(),
                    compare_with: ExpVal::STR("2023-12-31 23:59:59".to_string()),
                    compare_as: "TEXT".to_string()
                }),
                ("Exp3", Exp {
                    column: "invoice_amount".to_string(),
                    operator: "<".to_string(),
                    compare_with: ExpVal::STR("1000".to_string()),
                    compare_as: "NUMBERS".to_string()
                }),
                ("Exp4", Exp {
                    column: "address".to_string(),
                    operator: "FUZZ_MIN_SCORE_60".to_string(),
                    compare_with: ExpVal::VEC(vec!["public school".to_string()]),
                    compare_as: "TEXT".to_string()
                })
            ],
            "Exp1 && (Exp2 || Exp3) && Exp4",
        )
        .print_row_count()
        .save_as("/path/to/modified/file.csv");

#### Chainable Options

    use rgwml::csv_utils::{CalibConfig, CsvBuilder, CsvConverter, Exp, ExpVal, Train};
    use rgwml::xgb_utils::XgbConfig;
    use rgwml::dask_utils::{DaskGrouperConfig, DaskPivoterConfig, DaskCleanerConfig, DaskJoinerConfig, DaskIntersectorConfig, DaskDifferentiatorConfig};

    CsvBuilder::from_csv("/path/to/your/file1.csv")
    // A. Calibrating an irrugularly formatted file
    .calibrate(
        CalibConfig {
            header_is_at_row: "21".to_string(),
            rows_range_from: ("23".to_string(), "*".to_string())
        }) // sets the row 21 content as the header, and row 23 to last row content as the data

    // B. Setting and adding headers
    .set_header(vec!["Header1", "Header2", "Header3"])
    .add_column_header("NewColumn1")
    .add_column_headers(vec!["NewColumn2", "NewColumn3"])

    // C. Set an Index
    .resequence_id_column("account_id") // Sets the values of the specified column sequentially from 1 onwards, ensuring each entry is uniquely numbered in ascending order until the last row.
    
    // D. Assuming a single row csv, set the value of a column
    .set("column_name", "value");

    // E. Ordering columns
    .order_columns(vec!["Column1", "...", "Column5", "Column2"])
    .order_columns(vec!["...", "Column5", "Column2"])
    .order_columns(vec!["Column1", "Column5", "..."])

    // F. Overriding data from another builder object
    .override_with(other_csv_builder_object);

    // G. Modifying columns
    .drop_columns(vec!["Column1", "Column3"])
    .retain_columns(vec!["Column1", "Column3"])
    .rename_columns(vec![("Column1", "NewColumn1"), ("Column3", "NewColumn3")])

    // H. Adding and modifying rows
    .add_row(vec!["Row1-1", "Row1-2", "Row1-3"])
    .add_rows(vec![vec!["Row1-1", "Row1-2", "Row1-3"], vec!["Row2-1", "Row2-2", "Row2-3"]])
    .update_row_by_row_number(2, vec!["Bob", "36", "San Francisco"])
    .update_row_by_id(2, vec!["Bob", "36", "San Francisco"]) // Updates a row by id in the CSV, assuming the first column is 'id'
    .delete_row_by_row_number(2)
    .delete_row_by_id(2) // Deletes a row by id in the CSV, assuming the first column is 'id'
    .remove_duplicates()
    
    // I. Cleaning/ Replacing Cell values
    .trim_all() // Trims white spaces at the beginning and end of all cells in all columns.
    .replace_header_whitespaces_with_underscores()
    .replace_all(vec!["Column1".to_string(), "Column2".to_string()], vec![("null".to_string(), "".to_string()), ("NA".to_string(), "-".to_string())]) // In specified columns
    .replace_all(vec!["*".to_string()], vec![("null".to_string(), "".to_string()), ("NA".to_string(), "-".to_string())]) // In all columns
    replace_all_empty_string_cells_with(vec!["Column1", "Column2"], "0")
    .clean_or_test_clean_by_eliminating_rows_subject_to_column_parse_rules(
        DaskCleanerConfig {
            rules: "Column1:IS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER;Column2:IS_NUMERICAL_VALUE".to_string(),  // Avalable Rules: IS_NUMERICAL_VALUE, IS_POSITIVE_NUMERICAL_VALUE, IS_LENGTH:n (for instance: IS_LENGTH:9), IS_MIN_LENGTH:n, IS_MAX_LENGTH:n, IS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER, IS_NOT_AN_EMPTY_STRING, IS_DATETIME_PARSEABLE
            action: "ANALYZE_AND_CLEAN".to_string(), // Avalailable Actions: CLEAN, ANALYZE, ANALYZE_AND_CLEAN
            show_unclean_values_in_report: "TRUE".to_string(), // Options: TRUE, FALSE
        })
 
    // J. Limiting and sorting
    .limit(10)
    .limit_distributed_raw(10)  //  limit rows distributed as evenly as possible across the dataset
    .limit_distributed_category(10, "Colum7")  //  limit rows distributed as evenly as possible across the dataset, to maximize variance in values of the indicated column
    .limit_rand(10)         // limit rows randomly
    .limit_where(
        10,
        vec![
            ("Exp1", Exp {
                column: "Withdrawal Amt.".to_string(),
                operator: "<".to_string(),
                compare_with: ExpVal::STR("1000".to_string()),
                compare_as: "NUMBERS".to_string() // Also: "TEXT", "TIMESTAMPS"
            }),
            ("Exp2", Exp {
                column: "Withdrawal Type".to_string(),
                operator: "==".to_string(),
                compare_with: ExpVal::STR("Urgent".to_string()),
                compare_as: "TEXT".to_string()
            }),
        ],
        "Exp1 && Exp2",
        "TAKE:FIRST" // Also: TAKE:LAST, TAKE:RANDOM
        )
    .cascade_sort(vec![("Column1".to_string(), "DESC".to_string()), ("Column3".to_string(), "ASC".to_string())])

    // K. Search operations
    .print_contains_search_results("needle") // Prints rows where any cell contains the needle
    .print_not_contains_search_results("needle") // Prints rows where no cell contains the needle
    .print_starts_with_search_results("needle") // Prints rows where any cell starts with the needle
    .print_not_starts_with_search_results("needle") // Prints rows where no cell starts with the needle

    // L. Search operations
    .print_contains_search_results("needle", vec!["*"]) // Prints rows where any cell in all columns contains the needle
    .print_contains_search_results("needle", vec!["column1", "column2"]) // Same as above, but only specific columns targetted
    .print_not_contains_search_results("needle", vec!["*"]) // Prints rows where no cell in all columns contains the needle
    .print_not_contains_search_results("needle", vec!["column1", "column2"]) // Same as above, but only specific columns targetted
    .print_starts_with_search_results("needle", vec!["*"]) // Prints rows where any cell in all columns starts with the needle
    .print_starts_with_search_results("needle", vec!["column1", "column2"]) // Same as above, but only specific columns targetted
    .print_not_starts_with_search_results("needle", vec!["*"]) // Prints rows where no cell in all columns starts with the needle
    .print_not_starts_with_search_results("needle", vec!["column1", "column2"]) // Same as above, but only specific columns targetted
    .print_raw_levenshtein_search_results("needle", 10, ["column1", "column2"]) // Prints rows where cells in column1, column2 have a levenshtein distance of less than 10 vis-a-vis the needle
    .print_vectorized_levenshtein_search_results(["awesome", "good job"], max_lev_distance, ["column1", "column2"]) // Dynamically compares each needle against successive combinations of words within the cell values from the indicated columns, considering the minimum word count of the needle. It computes the Levenshtein distance for each needle qua the cell value, and for each such comparison the cell value is considered based on every combination of constituent words accruing from the minimum distance found within a specified maximum distance (max_lev_distance). This approach allows matching based on the proximity of words, providing a more contextually relevant search. For instance, if the cell contains "django is a good boy", it generates and compares distances for combinations like "django is", "is a", "a good", "good boy", up to the full cell content, ultimately considering the closest match. The minimum levenshtein distance acorss all needles for that cell value is then considered as the basis for filtering.

    // M. Applying conditional operations
    .where_(
        vec![
            ("Exp1", Exp {
                column: "customer_type".to_string(),
                operator: "==".to_string(),
                compare_with: ExpVal::STR("REGULAR".to_string()),
                compare_as: "TEXT".to_string() // Also: "NUMBERS", "TIMESTAMPS"
            }),
            ("Exp2", Exp {
                column: "invoice_data".to_string(),
                operator: ">".to_string(),
                compare_with: ExpVal::STR("2023-12-31 23:59:59".to_string()),
                compare_as: "TEXT".to_string()
            }),
            ("Exp3", Exp {
                column: "invoice_amount".to_string(),
                operator: "<".to_string(),
                compare_with: ExpVal::STR("1000".to_string()),
                compare_as: "NUMBERS".to_string()
            }),
            ("Exp4", Exp {
                column: "address".to_string(),
                operator: "FUZZ_MIN_SCORE_60".to_string(),
                compare_with: ExpVal::VEC(vec!["public school".to_string()]),
                compare_as: "TEXT".to_string()
            }),
            ("Exp5", Exp {
                column: "status".to_string(),
                operator: "CONTAINS".to_string(), // Also: "DOES_NOT_CONTAIN"
                compare_with: ExpVal::STR("REJECTED".to_string()),
                compare_as: "TEXT".to_string()
            }),
            ("Exp6", Exp {
                column: "status".to_string(),
                operator: "STARTS_WITH".to_string(), // Also: "DOES_NOT_START_WITH"
                compare_with: ExpVal::STR("VERIFIED".to_string()),
                compare_as: "TEXT".to_string()
            }),
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6")
    .where_set(
        vec![
            // Same as .where() 
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6",
        "Column10",
        "IS OKAY")

    // N. Analytical Prints for data inspection

    .print_columns()
    .print_row_count()
    .print_first_row("75").await // Shows the first row via a dask vectorization for object sizes of more than 75 mb, and via a String approach for smaller tables
    .print_first_row_small_file()
    .print_first_row_big_file().await

    .print_last_row("75").await // Shows the last row via a dask vectorization for object sizes of more than 75 mb, and via a String approach for smaller tables
    .print_last_row_small_file()
    .print_last_row_big_file().await

    .print_first_n_rows("2", "75").await // Shows the first 2 rows via a dask vectorization for object sizes of more than 75 mb, and via a String approach for smaller tables
    .print_first_n_rows_small_file("2")
    .print_first_n_rows_big_file("2").await 

    .print_last_n_rows("2", "75").await // Shows the last 2 rows via a dask vectorization for object sizes of more than 75 mb, and via a String approach for smaller tables
    .print_last_n_rows_small_file("2")
    .print_last_n_rows_big_file("2").await

    .print_rows_range("2","5","75").await // Shows results per a spreadsheet row range via a dask vectorization for object sizes of more than 75 mb, and via a String approach for smaller tables
    .print_rows_range_small_file("2","5")
    .print_rows_range_big_file("2","5").await
    .print_rows() // Shows results as per a spreadsheet row range
    .print_rows_where(
        vec![
            // Same as .where()
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6")
    .print_table("75").await // Prints a truncated table to the terminal, via a dask vectorization for object sizes of more than 75 mb, and via a String approach for smaller tables
    .print_table_big_file().await // Uses a dask vectorization output efficiently
    .print_table_small_file().await // Uses a String approach to output efficiently
    .print_table_all_rows() // Prints a truncated table to the terminal, with all rows
    .print_cells(vec!["Column1", "Column2"])
    .print_unique("column_name")
    .print_unique_count("column_name")
    .print_column_numerical_analysis(vec!["Column1", "Column2"]) // Prints the min, max, range, mean, median, mode, variance, standard deviation, sum of squared deviations, and list non-numerical values, if any, for each of the indicated columns
    .print_freq(vec!["Column1", "Column2"])
    .print_freq_cascading(vec!["Column1", "Column2"]) // Prints cascading frequency tables for selected columns of a dataset.
    .print_freq_mapped(vec![
            ("Column1", vec![
                ("Delhi", vec!["New Delhi", "Delhi"]),
                ("UP", vec!["Ghaziabad", "Noida"])
            ]),
            ("Column2", vec![("NO_GROUPINGS", vec![])])
        ])
    .print_unique_values_stats(vec!["Column1", "Column2"]) // Prints the number of unique values in a column, along with the mean and median of their frequencies
    .print_count_where(
        vec![
            // Same as .where()
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6")

    // O. Transforming Data
    .transpose_transform() // Transposes the headers with the first row
    .split_as("ColumnNameToGroupBy", "/output/folder/for/grouped/csv/files/") // Groups data by a specified column and saves each group into a separate CSV file in a given folder
    .grouped_index_transform(
        DaskGrouperConfig {
            group_by_column_name: "Column7".to_string(),
            count_unique_agg_columns: "".to_string(),
            numerical_max_agg_columns: "Column8, Column9".to_string(), 
            numerical_min_agg_columns: "".to_string(),
            numerical_sum_agg_columns: "".to_string(),
            numerical_mean_agg_columns: "".to_string(),
            numerical_median_agg_columns: "".to_string(),
            numerical_std_deviation_agg_columns: "".to_string(),
            mode_agg_columns: "".to_string(),
            datetime_max_agg_columns: "".to_string(),
            datetime_min_agg_columns: "".to_string(),
            datetime_semi_colon_separated_agg_columns: "".to_string(),
            bool_percent_agg_columns: "".to_string(),
        })
    .pivot(
        DaskPivoterConfig {
            group_by_column_name: "Column7".to_string(),
            values_to_aggregate_column_name: "Column9".to_string(),
            operation: "NUMERICAL_MEAN".to_string(), // Options: COUNT, COUNT_UNIQUE, NUMERICAL_MAX, NUMERICAL_MIN, NUMERICAL_SUM, NUMERICAL_MEAN, NUMERICAL_MEDIAN, NUMERICAL_STANDARD_DEVIATION, BOOL_PERCENT
            segregate_by_column_names: "Column3, Column5".to_string()
        })

    // P. Basic Set Theory Operations 
   
    // P.1. WITH CSV FILES (DYNAMIC THRESHOLD)
    .union_with_csv_file("/path/to/set_b/file.csv", 
        "UNION", // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
        "id",    // Table A reference column
        "id",    // Table B reference column
        "75"     // If the file size is above 75 MB, Dask vectorization will be favored over a String based approach
        ).await
    .intersection_with_csv_file("/path/to/set_b/file.csv",    // Analogus to 'INNER_JOIN' 
        "id",    // Table A reference column
        "id",    // Table B reference column
        "75"     // If the file size is above 75 MB, Dask vectorization will be favored over a String based approach
        ).await
    .difference_with_csv_file("/path/to/set_b/file.csv",     
        "NORMAL", // Options: NORMAL, SYMMETRIC
        "id",    // Table A reference column
        "id",    // Table B reference column
        "75"     // If the file size is above 75 MB, Dask vectorization will be favored over
 a String based approach
        ).await

    // P.2. WITH CSV BUILDER (DYNAMIC THRESHOLD)
    .union_with_csv_builder(set_b_csv_builder,     
        "UNION", // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
        "id",    // Table A reference column
        "id",    // Table B reference column
        "75"     // If the builder size is above 75 MB, Dask vectorization will be favored over a String based approach
        ).await
    .intersection_with_csv_builder(set_b_csv_builder,    // Analogus to 'INNER_JOIN'
        "id",    // Table A reference column
        "id",    // Table B reference column
        "75"     // If the builder size is above 75 MB, Dask vectorization will be favored over a String based approach
        ).await
    .difference_with_csv_builder(set_b_csv_builder,
        "NORMAL", // Options: NORMAL, SYMMETRIC
        "id",    // Table A reference column
        "id",    // Table B reference column
        "75"     // If the builder size is above 75 MB, Dask vectorization will be favored over a String based approach
        ).await

    // P.3. WITH CSV FILES (SMALL)
    .union_with_csv_file_small("/path/to/set_b/file.csv",
        "UNION", // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
        "id",    // Table A reference column
        "id",    // Table B reference column
        ).await
    .intersection_with_csv_file_small("/path/to/set_b/file.csv",    // Analogus to 'INNER_JOIN'
        "id",    // Table A reference column
        "id",    // Table B reference column
        ).await
    .difference_with_csv_file_small("/path/to/set_b/file.csv",
        "NORMAL", // Options: NORMAL, SYMMETRIC
        "id",    // Table A reference column
        "id",    // Table B reference column
        ).await

    // P.4. WITH CSV BUILDER (SMALL)
    .union_with_csv_builder_small(set_b_csv_builder,
        "UNION", // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
        "id",    // Table A reference column
        "id",    // Table B reference column
        ).await
    .intersection_with_csv_builder_small(set_b_csv_builder,    // Analogus to 'INNER_JOIN'
        "id",    // Table A reference column
        "id",    // Table B reference column
        ).await
    .difference_with_csv_builder_small(set_b_csv_builder,
        "NORMAL", // Options: NORMAL, SYMMETRIC
        "id",    // Table A reference column
        "id",    // Table B reference column
        ).await

    // P.5. WITH CSV FILES (BIG)
    .union_with_csv_file_big("/path/to/set_b/file.csv", 
        DaskJoinerConfig {
            join_type: "LEFT_JOIN".to_string(), // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
            table_a_ref_column: "id".to_string(), // Leave as empty "" for UNION/ BAG_UNION
            table_b_ref_column: "id".to_string(), // Leave as empty "" for UNION/ BAG_UNION
        }).await
    .intersection_with_csv_file_big("/path/to/set_b/file.csv", 
        DaskIntersectorConfig {
            table_a_ref_column: "id".to_string(), 
            table_b_ref_column: "id".to_string(),
        }).await
    .difference_with_csv_file_big("/path/to/set_b/file.csv",
        DaskDifferentiatorConfig {
            difference_type: "NORMAL".to_string(), // Options: NORMAL, SYMMETRIC
            table_a_ref_column: "id".to_string(), 
            table_b_ref_column: "id".to_string(), 
        }).await

    // P.6. WITH CSV BUILDER (BIG)
    .union_with_csv_builder_big(set_b_csv_builder, 
        DaskJoinerConfig {
            join_type: "LEFT_JOIN".to_string(), // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
            table_a_ref_column: "id".to_string(), // Leave as empty "" for UNION/ BAG_UNION
            table_b_ref_column: "id".to_string(), // Leave as empty "" for UNION/ BAG_UNION
        }).await
    .intersection_with_csv_file_big(set_b_csv_builder,
        DaskIntersectorConfig {
            table_a_ref_column: "id".to_string(), 
            table_b_ref_column: "id".to_string(), 
        }).await
    .difference_with_csv_file_big(set_b_csv_builder,
        DaskDifferentiatorConfig {
            difference_type: "NORMAL".to_string(), // Options: NORMAL, SYMMETRIC
            table_a_ref_column: "id".to_string(),
            table_b_ref_column: "id".to_string(),
        }).await

    // Q. Append Analytical Columns
    .append_static_value_column("static_value_across_all_rows", "new_column_name")
    .append_derived_boolean_column(
        "is_qualified_for_discount",
        vec![
            // Same as .where() 
        ],
        "Exp1 && (Exp2 || Exp3 || Exp4) && Exp5 && Exp6")
    .append_inclusive_exclusive_numerical_interval_category_column(
            "column_name",           // Values in this column should be numerically parseable
            "interval_points",       // For instance 0,5,10,15 .... and so on, will create the intervals 00 to 05, 05 to 10, 10 to 15 ... and so on, with the lower limit being inclusive  , and the upper limit being exclusive
            "new_column_name"        // The name of the new column 
        )
    .append_inclusive_exclusive_numerical_interval_category_column(
            "column_name",           // Values in this column should be numerically parseable
            "interval_points",       // For instance 0,5,10,15 .... and so on, will create the intervals 00 to 05, 05 to 10, 10 to 15 ... and so on, with the lower limit being inclusive , and the upper limit being exclusive
            "new_column_name"        // The name of the new column
        )
    .append_derived_category_column(
        "EXPENSE_RANGE",
        vec![
            (
                "< 1000",
                vec![
                    ("Exp1", Exp {
                        column: "Withdrawal Amt.".to_string(),
                        operator: "<".to_string(),
                        compare_with: ExpVal::STR("1000".to_string()),
                        compare_as: "NUMBERS".to_string() // Also: "TEXT", "TIMESTAMPS"
                    }),
                ],
                "Exp1"
            ),
            (
                "1000-5000",
                vec![
                    ("Exp1", Exp {
                        column: "Withdrawal Amt.".to_string(),
                        operator: ">=".to_string(),
                        compare_with: ExpVal::STR("1000".to_string()),
                        compare_as: "NUMBERS".to_string()
                    }),
                    ("Exp2", Exp {
                        column: "Withdrawal Amt.".to_string(),
                        operator: "<".to_string(),
                        compare_with: ExpVal::STR("5000".to_string()),
                        compare_as: "NUMBERS".to_string()
                    }),
                ],
                "Exp1 && Exp2"
            )
        )
    .append_derived_concatenation_column("NewColumnName", vec!["Column1", " ", "Column2", "@"]) // Items in the vector that are not column names will be concatenated as strings
    .append_derived_openai_analysis_columns(
        vec!["column7", "column9"],     // Names of the columns to be analyzed 
        std::collections::HashMap::from([
            ("noun".to_string(), "extract the noun from the sentence".to_string()),
            ("verb".to_string(), "extract the verb from the sentence".to_string()),
        ]),
        "YOUR_OPEN_AI_API_KEY",
        "gpt-3.5-turbo-0125"            // Any OpenAI model with the JSON mode feature
        ).await
    .append_derived_smartcore_linear_regression_column(
        "predictions",                  // name of new column to store predictions
        vec![                           // predictor combinations/ feature sets - length should be 2x the number of predictors/features
            vec!["90", "good"],         // predictor/ feature values can also be text strings. The model uses a Levenshtein distance based approach to tokenize strings.
            vec!["70", "bad"], 
            vec!["60", "great"], 
            vec!["40", "awful"]
        ], 
        vec![72.0, 65.0, 63.0, 56.0],   // labels mapped to the above predictors
        vec![0.0, 100.0],               // normalization range of minimum and maximum prediction value
        vec!["Column1", "Column7"])     // names of columns whose values are to be used to make predictions as the 'test' data set 
    .append_openai_batch_analysis_columns(
        "YOUR_OPEN_AI_API_KEY",
        "output_file_id"
        )
    .append_fuzzai_analysis_columns(
        "Column1", // Name of column to be analyzed
        "sales_analysis", // Identifier for newly created columns
        vec![
            Train {
                input: "I want my money back".to_string(),
                output: "refund".to_string()
            },
            Train {
                input: "I want a refund immediately".to_string(),
                output: "refund".to_string()
            },
        ],
        "WORD_SPLIT:2", // The minimum length of word combinations that training data is to be broken into
        "WORD_LENGTH_SENSITIVITY:0.8", // Multiplies differences in word length between training data input and the value being analyzed by 0.8
        "GET_BEST:2" // Get the top 2 results, max value is 3
        )
    .append_fuzzai_analysis_columns_with_values_where(
        "Column1", // Name of column to be analyzed
        "sales_analysis", // Identifier for newly created column
        vec![
            Train {
                input: "I want my money back".to_string(),
                output: "refund".to_string()
            },
            Train {
                input: "I want a refund immediately".to_string(),
                output: "refund".to_string()
            },
        ],
        "WORD_SPLIT:2", // The minimum length of word combinations that training data is to be broken into
        "WORD_LENGTH_SENSITIVITY:0.8", // Multiplies differences in word length between training data input and the value being analyzed by 0.8
        "GET_BEST:2", // Get the top 2 results, max value is 3
        vec![
            ("Exp1", Exp {
                column: "Deposit Amt.".to_string(),
                operator: ">".to_string(),
                compare_with: ExpVal::STR("500".to_string()),
                compare_as: "NUMBERS".to_string() // Also: "TEXT", "TIMESTAMPS"
            }),
        ],
        "Exp1", // Filters rows where fuzzai analysis would be applied
        )

    // R. Append Analytical Date/Timestamp Columns
    .append_semi_colon_separated_timestamp_count_after_date_column(
        "semi_colon_separated_timestamps_column_name", 
        "date_column_name", 
        "new_column_name"
        )   
    .append_semi_colon_separated_timestamp_count_before_date_column(
        "semi_colon_separated_timestamps_column_name", 
        "date_column_name", 
        "new_column_name"
        )   
    .append_added_days_column_relative_to_adjacent_column(
        "days_column_name",         // Should be float parseable
        "timestamp_column_name",    // Should be timestamp/date parseable
        "new_column_name"
        )
    .append_subtracted_days_column_relative_to_adjacent_column(
        "days_column_name",         // Should be float parseable
        "timestamp_column_name",    // Should be timestamp/date parseable
        "new_column_name"
        )
    .append_added_days_column(
        "date_column_name",             // Should be timestamp/date parseable
        "number_of_days_to_add",        // Should be float parseable
        "new_column_name"
        )
    .append_subtracted_days_column(
        "date_column_name",              // Should be timestamp/date parseable
        "number_of_days_to_subtract",    // Should be float parseable
        "new_column_name"
        )
    .append_day_difference_column(
        "date_column_1_name",
        "date_column_2_name",
        "new_column_name"
        )
    .split_date_as_appended_category_columns("Column10", "%d/%m/%y") // Appends additional columns splitting a date/timestamp into categorization columns by year, month and week


    // S. Plot charts
    .print_dot_chart("Column3", "Column5") // X axis column followed by the Y axis column
    .print_cumulative_dot_chart("Column3", "Column5") // X axis column followed by the Y axis column
    .print_smooth_line_chart("Column3", "Column5") // X axis column followed by the Y axis column
    .print_cumulative_smooth_line_chart("Column3", "Column5") // X axis column followed by the Y axis column

    // T. Save
    .save_as("/path/to/your/file2.csv")

    // U. Die
    .die() // Gracefully terminates execution of a CsvBuilder chain

#### Extract Data

These methods return specific data, instead of a mutable CsvBuilder object, and hence, can not be subsequently chained.

    let builder = CsvBuilder::from_csv("/path/to/your/file1.csv");

    builder
    .get_unique("column_name"); // Returns a Vec<String>
    .get("column_name"); // Returns cell content as a String, if the csv has been filtered to single row. See the chainable ".set()" method above for set a value in such a circumstance
    .get_freq(vec!["Column1", Column2]) // Returns a HashMap where keys are column names and values are vectors of sorted (value, frequency) pairs.
    .get_freq_mapped(vec![
            ("Column1", vec![
                ("Delhi", vec!["New Delhi", "Delhi"]),
                ("UP", vec!["Ghaziabad", "Noida"])
            ]),
            ("Column2", vec![("NO_GROUPINGS", vec![])])
        ])
    .has_data() // Returns `true` if either headers or data rows are present, `false` otherwise.
    .has_headers() // Returns `true` if headers are present, `false` otherwise.
    .get_headers().unwrap() // Returns an Option<&[String]> containing a reference to the headers if present, `None` otherwise.
    .get_data().unwrap() // Returns an Option<&Vec<Vec<String>>> containing a reference to the data contained in the builder.

    .get_numeric_min("Column1").unwrap() // Returns a String value of the minimum numeric value - assuming all values of the column can be consistently parsed as such
    .get_numeric_max("Column1").unwrap() // Returns a String value of the maximum numeric value - assuming all values of the column can be consistently parsed as such
    .get_datetime_min("Column1").unwrap() // Returns a String value of the minimum numeric value - assuming all values of the column can be consistently parsed as such
    .get_datetime_max("Column1").unwrap() // Returns a String value of the maximum numeric value - assuming all values of the column can be consistently parsed as such
    .get_range("Column1").unwrap() // Returns an `Option<f64>` the range (difference between the maximum and minimum) in a numerically parseable column. 
    .get_sum("Column1").unwrap() // Returns an `Option<f64>` the sum of all values in a numerically parseable column.
    .get_mean("Column1").unwrap() // Returns an `Option<f64>` - the mean of all values in a numerically parseable column.
    .get_median("Column1").unwrap() // Returns an `Option<f64>` - the median of all values in a numerically parseable column.
    .get_mode("Column1").unwrap() // Returns an `Option<f64>` - the mode of all values in a numerically parseable column.
    .get_variance("Column1").unwrap() // Returns an `Option<f64>` - the variance of all values in a numerically parseable column.
    .get_standard_deviation("Column1").unwrap() // Returns an `Option<f64>` - the standard deviation of all values in a numerically parseable column.
    .get_sum_of_squared_deviations("Column1").unwrap() // Returns an `Option<f64>` - the getsum of squared deviations of all values in a numerically parseable column.
    .get_get_non_numeric_values("Column1").unwrap() // Returns an `Option<Vec<String>>` - the non numeric values in a column. 

    // Send data to OpenAI for batch analysis, returning a batch_id as `Result<String, Box<dyn std::error::Error>>`
    .send_columns_for_openai_batch_analysis(
        vec!["column7", "column9"],     // Names of the columns to be analyzed
        std::collections::HashMap::from([
            ("noun".to_string(), "extract the noun from the sentence".to_string()),
            ("verb".to_string(), "extract the verb from the sentence".to_string()),
        ]),
        "YOUR_OPEN_AI_API_KEY",
        "gpt-3.5-turbo-0125"            // Any OpenAI model with the JSON mode feature
        "night_job"                     // Name of the batch

    )
    .get_all_csv_files("path/to/your/directory") // Returns a `Result<CsvBuilder, Box<dyn Error>>` of all CSV files in the directory specifying their file name, last modified time, and file size in mb

#### CsvBuilder-XGBoost Operations

    // A. Creating a Model
    use rgwml::csv_utils::CsvBuilder;
    use rgwml::xgb_utils::XgbConfig;

    let mut builder = CsvBuilder::from_csv("/path/to/your/training_data.csv");

    let (builder, report) =  builder.create_xgb_model(
        "ticket_count, tickets_after_last_payment",     // Param column names
        "churn_day",                                    // Target column name
        "PREDICTION",                                   // Prediction column name
        "/home/rgw/Desktop/csv_db/xgb_models",          // Dir to save model
        "churn_v8",                                     // Model name without extension
        XgbConfig {
            objective: "reg:squarederror".to_string(), // Leave as empty string for binary classification
            xgb_max_depth: "".to_string(),
            xgb_learning_rate: "".to_string(),
            xgb_n_estimators: "".to_string(),
            xgb_gamma: "".to_string(),
            xgb_min_child_weight: "".to_string(),
            xgb_subsample: "".to_string(),
            xgb_colsample_bytree: "".to_string(),
            xgb_reg_lambda: "".to_string(),
            xgb_reg_alpha: "".to_string(),
            xgb_scale_pos_weight: "".to_string(),
            xgb_max_delta_step: "".to_string(),
            xgb_booster: "".to_string(),
            xgb_tree_method: "".to_string(),
            xgb_grow_policy: "".to_string(),
            xgb_eval_metric: "".to_string(),
            xgb_early_stopping_rounds: "".to_string(),
            xgb_device: "".to_string(),
            xgb_cv: "".to_string(),
            xgb_interaction_constraints: "".to_string(),
            hyperparameter_optimization_attempts: "".to_string(),
            hyperparameter_optimization_result_display_limit: "".to_string(),
            dask_workers: "".to_string(),
            dask_threads_per_worker: "".to_string(),
        },
    )
    .await;

    builder
        .order_columns(vec!["...", "churn_day", "PREDICTION"])
        .print_table();
    dbg!(report);

    // B. Invoking a Model
    use rgwml::csv_utils::CsvBuilder;
    use rgwml::xgb_utils::XgbConfig;

    let mut builder = CsvBuilder::from_csv("/home/rgw/Desktop/csv_db/churn_predictions_data.csv");

    builder.append_xgb_model_predictions_column(
        "ticket_count, tickets_after_last_payment",             // Param column names
        "PREDICTION",                                           // Prediction column name
        "/path/to/your/model/churn_v8.json"     // Model absolute path
    ).await;

    let _ = builder
        .order_columns(vec!["...", "ticket_count", "tickets_after_last_payment", "PREDICTION"])
        .print_table()
        .save_as("/path/to/your/predictions/churn_v8_predictions_current_portfolio.csv");

#### CsvBuilder-Clustering (scikit-learn) Operations

    // A. Creating a Model
    use rgwml::csv_utils::CsvBuilder;
    use rgwml::clustering_utils::ClusteringConfig;
    use tokio::runtime::Runtime;
    use std::path::PathBuf;
    use std::env::current_dir;
    
    let headers = vec![
        "customer_id".to_string(),
        "age".to_string(),
        "annual_income".to_string(),
        "spending_score".to_string(),
    ];
    
    let data = vec![
        vec!["1".to_string(), "19".to_string(), "15".to_string(), "39".to_string()],
        vec!["2".to_string(), "21".to_string(), "15".to_string(), "81".to_string()],
        vec!["3".to_string(), "20".to_string(), "16".to_string(), "6".to_string()],
        vec!["4".to_string(), "23".to_string(), "16".to_string(), "77".to_string()],
        vec!["5".to_string(), "31".to_string(), "17".to_string(), "40".to_string()],
        vec!["6".to_string(), "22".to_string(), "17".to_string(), "76".to_string()],
        vec!["7".to_string(), "35".to_string(), "18".to_string(), "6".to_string()],
        vec!["8".to_string(), "23".to_string(), "18".to_string(), "94".to_string()],
        vec!["9".to_string(), "64".to_string(), "19".to_string(), "3".to_string()],
        vec!["10".to_string(), "30".to_string(), "19".to_string(), "72".to_string()],
    ];
    
    let mut builder = CsvBuilder::from_raw_data(headers, data);
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let param_column_names = "age, annual_income, spending_score";
        let cluster_column_name = "CLUSTERING";
        let clustering_config = ClusteringConfig {
            operation: "KMEANS".to_string(),        // Options: KMEANS, DBSCAN, AGGLOMERATIVE, MEAN_SHIFT, GMM, SPECTRAL, BIRCH
            optimal_n_cluster_finding_method: "ELBOW".to_string(),  // FIXED:{n}, ELBOW, SILHOUETTE; Not relevant for MEAN_SHIFT and DBSCAN
            dbscan_eps: "".to_string(),             // Only relevant for DBSCAN
            dbscan_min_samples: "".to_string(),     // Only relevant for DBSCAN
        };
        builder.append_clustering_column(param_column_names, cluster_column_name, clustering_config).await;
    });

### CsvConverter

    use serde_json::json;
    use tokio;
    use rgwml::csv_utils::CsvConverter;
    use rgwml::api_utils::ApiCallBuilder;

    // Function to fetch sales data from an API
    async fn fetch_sales_data_from_api() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/sales"; // API URL to fetch sales data

        // Payload for the API call
        let payload = json!({
            "date": "2023-12-21"
        });

        // Performing the API call
        let response = ApiCallBuilder::call(method, url, None, Some(payload))
            .execute()
            .await?;

        Ok(response)
    }

    // Main function with tokio's async runtime
    #[tokio::main]
    async fn main() {
        // Fetch sales data and handle potential errors inline
        let sales_data_response = fetch_sales_data_from_api().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch sales data: {}", e);
            std::process::exit(1); // Exit the program in case of an error
        });

        // Convert the fetched JSON data to CSV
        CsvConverter::from_json(&sales_data_response, "path/to/your/file.csv")
            .expect("Failed to convert JSON to CSV"); // Handle errors in CSV conversion
    }

### Caching a CsvBuilder with the `was_last_modified_within` method

#### Example 1: Usage with an API Call

    use rgwml::api_utils::ApiCallBuilder;
    use rgwml::csv_utils::CsvBuilder;
    use serde_json::json;
    use tokio;

    async fn generate_daily_sales_report() -> Result<(), Box<dyn std::error::Error>> {
        async fn fetch_sales_data_from_api() -> Result<String, Box<dyn std::error::Error>> {
            let method = "POST";
            let url = "http://example.com/api/sales"; // API URL to fetch sales data

            let payload = json!({
                "date": "2023-12-21"
            });

            let response = ApiCallBuilder::call(method, url, None, Some(payload))
                .execute()
                .await?;

            Ok(response)
        }

        let sales_data_response = fetch_sales_data_from_api().await?;

        // Convert the JSON response to CSV format using CsvBuilder
        let mut csv_builder = CsvBuilder::new();
        csv_builder.set_header(vec!["column1", "column2", "column3"]); // Set your headers appropriately
        // Add your rows based on the API response
        // csv_builder.add_row(vec!["value1", "value2", "value3"]);

        csv_builder.save_as("/path/to/daily/sales/report/cache.csv");

        Ok(())
    }

    #[tokio::main]
    async fn main() {
        let cache_path = "/path/to/daily_sales_report.csv";
        let cache_duration_minutes = 1440; // Cache duration set to 1 day

        if !CsvBuilder::was_last_modified_within(cache_path, &cache_duration_minutes.to_string()) {
            if let Err(e) = generate_daily_sales_report().await {
                eprintln!("Failed to generate sales report: {}", e);
            }
        }

        println!("Sales report is ready.");
    }

#### Example 2: Usage with a CsvBuilder retrieved from an external database

    use rgwml::csv_utils::CsvBuilder;
    use tokio;

    async fn generate_builder(cache_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
        SELECT * FROM your_table
        "#;

        let mut builder = CsvBuilder::from_mysql_query(
            "username",
            "password",
            "host",
            "database",
            &query,
        )
        .await
        .expect("Failed to create CsvBuilder from query");

        builder.save_as(cache_path);

        Ok(())
    }

    #[tokio::main]
    async fn main() {
        let cache_path = "/path/to/cache/your/file.csv";
        let cache_duration_minutes = 1440; // Cache duration set to 1 day

        if !CsvBuilder::was_last_modified_within(cache_path, &cache_duration_minutes.to_string()) {
            if let Err(e) = generate_builder(cache_path).await {
                eprintln!("Failed to generate data: {}", e);
            }
        }

        let mut builder = CsvBuilder::from_csv(cache_path);
        builder.print_table();
    }


5. `heavy_csv_utils`
--------------------

### Instantiation

Example 1: Creating a new object

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let builder = HeavyCsvBuilder::heavy_new()
        .heavy_set_header(vec!["Column1".to_string(), "Column2".to_string(), "Column3".to_string()])
        .heavy_set_data(vec![
            vec![b"Row1-1".to_vec(), b"Row1-2".to_vec(), b"Row1-3".to_vec()],
            vec![b"Row2-1".to_vec(), b"Row2-2".to_vec(), b"Row2-3".to_vec()]
        ])
    .heavy_save_as("/path/to/your/file.csv");

    builder.heavy_print_table();

Example 2: Load from an existing file

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let builder = HeavyCsvBuilder::heavy_from_csv("/path/to/existing/file.csv");
    builder.heavy_print_table();

Example 3: Load from a publicly-viewable Google Sheets URL

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    use tokio::runtime::Runtime;

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let csv_builder = HeavyCsvBuilder::from_publicly_viewable_google_sheet("https://docs.google.com/spreadsheets/d/1U9ozNFwV__c15z4Mp_EWorGwOv6mZPaQ9dmYtjmCPow/edit#gid=272498272").await;

        csv_builder.heavy_print_table();
    });

Example 4: From a Bare Metal Python Executable

    # A bare metal python executable should:
    # - Be executable without an virtual environment, with the `python3 <file_name>.py <arguments>` format;
    # - Specify a --uid flag accepting a string value, for the library to retrieve the output correctly
    # - Save the output to rgwml_{uid}.json file
    # For instance:

    import os
    import argparse
    import json
    import mmap
    import pandas as pd

    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Process some data")
        parser.add_argument('--uid', type=str, help='A unique identifier to name the output json file', required=True)

        parser.add_argument('--file_a_path', type=str, required=True, help='Path to the first CSV file')
        parser.add_argument('--file_b_path', type=str, required=True, help='Path to the second CSV file')
        parser.add_argument('--join_type', type=str, required=True, choices=['LEFT_JOIN', 'RIGHT_JOIN', 'OUTER_FULL_JOIN', 'UNION', 'BAG_UNION'], help='Type of join operation to perform')
        parser.add_argument('--file_a_ref_column', type=str, required=False, help='Reference column in the first CSV file')
        parser.add_argument('--file_b_ref_column', type=str, required=False, help='Reference column in the second CSV file')


        # Some processing logic that creates `df`

        headers = df.columns.tolist()
        rows = df.values.tolist()

        output = {
            "headers": headers,
            "rows": [[str(item) for item in row] for row in rows],
        }

        json_output = json.dumps(output, indent=4)
        filename = f"rgwml_{uid}.json"

        with open(filename, 'wb') as f:
            f.write(b' ' * len(json_output))

        with open(filename, 'r+b') as f:
            mm = mmap.mmap(f.fileno(), 0)
            mm.write(json_output.encode('utf-8'))
            mm.close()

    // Now, you can load the result of the above directly into your RGWML workflow, in the manner shown below.

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    use tokio::runtime::Runtime;
    use std::path::PathBuf;

    let current_dir = std::env::current_dir().unwrap();
    let executable_path = current_dir.join("python_executables/dask_joiner_connect.py");
    let executable_path_str = executable_path.to_str().unwrap();

    // Append the file name to the directory path
    let csv_path_a = current_dir.join("test_file_samples/joining_test_files/join_file_a.csv");
    let csv_path_a_str = csv_path_a.to_str().unwrap();

    let csv_path_b = current_dir.join("test_file_samples/joining_test_files/join_file_b.csv");
    let csv_path_b_str = csv_path_b.to_str().unwrap();

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let args = vec![
            ("--file_a_path", csv_path_a_str),
            ("--file_b_path", csv_path_b_str),
            ("--join_type", "LEFT_JOIN"),
            ("--file_a_ref_column", "id"),
            ("--file_b_ref_column", "id")
        ];

        let mut builder = HeavyCsvBuilder::heavy_from_bare_metal_python_executable(
            &executable_path_str,
            args,
            ).await;

    });

Example 5: Load from xls/ xlsx/ h5 files

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    // Load from a sheet in an .xls file
    let builder_1 = HeavyCsvBuilder::heavy_from_xls("/path/to/existing/file.xls", "Sheet1", "SHEET_NAME"); // Loads from the sheet named "Sheet1" of the .xls file.
    builder_1.heavy_print_table();
    let builder_2 = HeavyCsvBuilder::heavy_from_xls("/path/to/existing/file.xls", "1", "SHEET_ID"); // Loads from the seond sheet of the .xls file i.e. having an id of 1 (since the first sheet has an id of 0).
    builder_2.heavy_print_table();

    // Load from a sheet in an .xlsx file
    let builder_1 = HeavyCsvBuilder::heavy_from_xlsx("/path/to/existing/file.xlsx", "Sheet1", "SHEET_NAME"); // Loads from the sheet named "Sheet1" of the .xlsx file.
    builder_1.heavy_print_table();
    let builder_2 = HeavyCsvBuilder::heavy_from_xlsx("/path/to/existing/file.xlsx", "1", "SHEET_ID"); // Loads from the seond sheet of the .xlsx file i.e. having an id of 1 (since the first sheet has an id of 0).       
    builder_2.heavy_print_table();

    // Load from a dataset in an .h5 file
    let builder_1 = HeavyCsvBuilder::heavy_from_h5("/path/to/existing/file.h5", "Dataset1", "DATASET_NAME").await; // Loads from the dataset named "Dataset1" of the .h5 file.
    builder_1.heavy_print_table();
    let builder_2 = HeavyCsvBuilder::heavy_from_h5("/path/to/existing/file.h5", "1", "DATASET_ID").await; // Loads from the seond sheet of the .h5 file i.e. having an id of 1 (since the first sheet has an id of 0).
    builder_2.heavy_print_table();

Example 6: Load from raw data

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let headers = vec!["Header1".to_string(), "Header2".to_string(), "Header3".to_string()];
    let data = vec![
        vec!["Row1-1".to_string(), "Row1-2".to_string(), "Row1-3".to_string()],
        vec!["Row2-1".to_string(), "Row2-2".to_string(), "Row2-3".to_string()],
    ];

    let builder = HeavyCsvBuilder::heavy_from_raw_data(headers, data);
    builder.heavy_print_table();

Example 7: Load from an MSSQL/MYSQL Server query

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::heavy_from_mssql_query(            // Also available: .from_mysql_query
        "username", 
        "password", 
        "server", 
        "database", 
        "SELECT * from your_table")
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 8: Load from an MSSQL/ MYSQL Server query, receiving the data in chunks, collated as a union

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::heavy_from_chunked_mssql_query_union(    // Also available: .from_chunked_mysql_query_union
        "username",
        "password",
        "server",
        "database",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 9: Load from an MSSQL/ MYSQL Server query, receiving the data in chunks, collated as a bag union

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::heavy_from_chunked_mssql_query_bag_union(    // Also available: .from_chunked_mysql_query_bag_union
        "username",
        "password",
        "server",
        "database", 
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 10: Load from a Clickhouse Server query

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::from_clickhouse_query(  
        "username",
        "password",
        "server",
        "SELECT * from your_table")
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 11: Load from a Clickhouse Server query, receiving the data in chunks, collated as a union

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::heavy_from_chunked_clickhouse_query_union(
        "username",
        "password",
        "server",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 12: Load from a Clickhouse Server query, receiving the data in chunks, collated as a bag union

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::heavy_from_chunked_clickhouse_query_bag_union(
        "username",
        "password",
        "server",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 13: Load from a Google Big Query Server

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::heavy_from_google_big_query_query(  
        "path/to/your/json/credentials",
        "SELECT * from your_table")
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 14: Load from a Google Big Query Server query, receiving the data in chunks, collated as a union

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::heavy_from_chunked_google_big_query_query_union(
        "path/to/your/json/credentials",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 15: Load from a Google Big Query Server query, receiving the data in chunks, collated as a bag union

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let result = HeavyCsvBuilder::heavy_from_chunked_google_big_query_query_bag_union(
        "path/to/your/credentials",
        "SELECT * from your_table"
        "10000" // Get data in chunks of 10000 rows at a time
        )
        .await
        .expect("Failed to create CsvBuilder query");

    result.heavy_print_table();

Example 16: Load a new instance from an existing instance

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;

    let builder_instance_1 = HeavyCsvBuilder::heavy_from_xls("/path/to/existing/file.xls", 1);
    let builder_instance_2 = HeavyCsvBuilder::heavy_from_copy(builder_instance_1);

### Chainable Options

    use rgwml::heavy_csv_utils::HeavyCsvBuilder;
    use rgwml::xgb_utils::XgbConfig;
    use rgwml::dask_utils::{DaskGrouperConfig, DaskPivoterConfig, DaskCleanerConfig, DaskJoinerConfig, DaskIntersectorConfig, DaskDifferentiatorConfig};

    HeavyCsvBuilder::heavy_from_csv("/path/to/your/file1.csv")
    // A. Setting Header and Data
    .heavy_set_header(vec!["Header1", "Header2", "Header3"])
    .heavy_set_data(vec![
        vec![b"Row1-1".to_vec(), b"Row1-2".to_vec(), b"Row1-3".to_vec()],
        vec![b"Row2-1".to_vec(), b"Row2-2".to_vec(), b"Row2-3".to_vec()]
    ])

    // B. Cleaning
    .heavy_clean_or_test_clean_by_eliminating_rows_subject_to_column_parse_rules(
        DaskCleanerConfig {
            rules: "Column1:IS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER;Column2:IS_NUMERICAL_VALUE".to_string(),  // Avalable Rules: IS_NUMERICAL_VALUE, IS_POSITIVE_NUMERICAL_VALUE, IS_LENGTH:n (for instance: IS_LENGTH:9), IS_MIN_LENGTH:n, IS_MAX_LENGTH:n, IS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER, IS_NOT_AN_EMPTY_STRING, IS_DATETIME_PARSEABLE
            action: "ANALYZE_AND_CLEAN".to_string(), // Avalailable Actions: CLEAN, ANALYZE, ANALYZE_AND_CLEAN
            show_unclean_values_in_report: "TRUE".to_string(), // Options: TRUE, FALSE
        })
    
    // C. Analytical Prints for data inspection

    .heavy_print_table()
    .heavy_print_first_row().await
    .heavy_print_last_row.await
    .heavy_print_first_n_rows("2").await 
    .heavy_print_last_n_rows("2").await
    .heavy_print_rows_range("2","5").await
    .heavy_print_freq(vec!["Column1", "Column2"])
    .heavy_print_freq_cascading(vec!["Column1", "Column2"]) // Prints cascading frequency tables for selected columns of a dataset.
    .heavy_print_unique_values_stats(vec!["Column1", "Column2"]) // Prints the number of unique values in a column, along with the mean and median of their frequencies

    // D. Transforming Data
    .heavy_grouped_index_transform(
        DaskGrouperConfig {
            group_by_column_name: "Column7".to_string(),
            count_unique_agg_columns: "".to_string(),
            numerical_max_agg_columns: "Column8, Column9".to_string(), 
            numerical_min_agg_columns: "".to_string(),
            numerical_sum_agg_columns: "".to_string(),
            numerical_mean_agg_columns: "".to_string(),
            numerical_median_agg_columns: "".to_string(),
            numerical_std_deviation_agg_columns: "".to_string(),
            mode_agg_columns: "".to_string(),
            datetime_max_agg_columns: "".to_string(),
            datetime_min_agg_columns: "".to_string(),
            datetime_semi_colon_separated_agg_columns: "".to_string(),
            bool_percent_agg_columns: "".to_string(),
        })
    .heavy_pivot(
        DaskPivoterConfig {
            group_by_column_name: "Column7".to_string(),
            values_to_aggregate_column_name: "Column9".to_string(),
            operation: "NUMERICAL_MEAN".to_string(), // Options: COUNT, COUNT_UNIQUE, NUMERICAL_MAX, NUMERICAL_MIN, NUMERICAL_SUM, NUMERICAL_MEAN, NUMERICAL_MEDIAN, NUMERICAL_STANDARD_DEVIATION, BOOL_PERCENT
            segregate_by_column_names: "Column3, Column5".to_string()
        })

    // E. Basic Set Theory Operations 
   
    // E.1. WITH CSV FILES
    .heavy_union_with_csv_file("/path/to/set_b/file.csv", 
        DaskJoinerConfig {
            join_type: "LEFT_JOIN".to_string(), // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
            table_a_ref_column: "id".to_string(), // Leave as empty "" for UNION/ BAG_UNION
            table_b_ref_column: "id".to_string(), // Leave as empty "" for UNION/ BAG_UNION
        }).await
    .heavy_intersection_with_csv_file("/path/to/set_b/file.csv", 
        DaskIntersectorConfig {
            table_a_ref_column: "id".to_string(), 
            table_b_ref_column: "id".to_string(),
        }).await
    .heavy_difference_with_csv_file("/path/to/set_b/file.csv",
        DaskDifferentiatorConfig {
            difference_type: "NORMAL".to_string(), // Options: NORMAL, SYMMETRIC
            table_a_ref_column: "id".to_string(), 
            table_b_ref_column: "id".to_string(), 
        }).await

    // E.2. WITH CSV BUILDER
    .heavy_union_with_csv_builder(set_b_csv_builder, 
        DaskJoinerConfig {
            join_type: "LEFT_JOIN".to_string(), // Options: UNION, BAG_UNION, LEFT_JOIN, RIGHT_JOIN, OUTER_FULL_JOIN
            table_a_ref_column: "id".to_string(), // Leave as empty "" for UNION/ BAG_UNION
            table_b_ref_column: "id".to_string(), // Leave as empty "" for UNION/ BAG_UNION
        }).await
    .heavy_intersection_with_csv_file(set_b_csv_builder,
        DaskIntersectorConfig {
            table_a_ref_column: "id".to_string(), 
            table_b_ref_column: "id".to_string(), 
        }).await
    .heavy_difference_with_csv_file(set_b_csv_builder,
        DaskDifferentiatorConfig {
            difference_type: "NORMAL".to_string(), // Options: NORMAL, SYMMETRIC
            table_a_ref_column: "id".to_string(),
            table_b_ref_column: "id".to_string(),
        }).await


    // F. Save
    .save_as("/path/to/your/file2.csv")

    // G. Die
    .die() // Gracefully terminates execution of a CsvBuilder chain

### Extract Data

These methods return specific data, instead of a mutable CsvBuilder object, and hence, can not be subsequently chained.

    let builder = HeavyCsvBuilder::heavy_from_csv("/path/to/your/file1.csv");

    builder
        .heavy_has_data() // Returns `true` if either headers or data rows are present, `false` otherwise.
        .heavy_has_headers() // Returns `true` if headers are present, `false` otherwise.
        .heavy_get_headers(); // Returns a reference to the headers as &Vec<String>
        .heavy_get_data(); // Returns the concatenated data as Vec<u8>
        .heavy_get_data_as_string(); // Returns the concatenated data as a String
        .heavy_get_row_as_vector_of_strings(row_number); // Returns a row as a vector of strings (Vec<String>)
        .heavy_get_data_as_vector_of_vector_strings(); // Returns the data as a vector of vectors of strings (Vec<Vec<String>>)

5. `db_utils`
-------------

### Easily query a MSSQL, MYSQL, Clickhouse server, or Google Big Query to extract data

    use rgwml::db_utils::DbConnect;

    #[tokio::main]
    async fn main() {
        let result_1 = DbConnect::execute_mssql_query( // use `execute_mysql_query` for MYSQL
            "username", 
            "password", 
            "server/host", 
            "database", 
            "SELECT * FROM your_table").await?;

        let headers_1 = result_1.0;
        let row_data_1 = result_1.1;

        let result_2 = DbConnect::execute_clickhouse_query( 
            "username",
            "password",
            "server/host",
            "SELECT * FROM your_table").await?;

        let headers_2 = result_2.0;
        let row_data_2 = result_2.1;

        let result_3 = DbConnect::execute_google_big_query_query(
            "your/json/credentials/path",
            "SELECT * FROM your_table").await?;

        let headers_3 = result_2.0;
        let row_data_3 = result_2.1;

    }

### Easily query a MYSQL server to write data

Easily query a MSSQL or MYSQL server to extract data

    use rgwml::db_utils::DbConnect;

    #[tokio::main]
    async fn main() {
        let result = DbConnect::execute_mysql_write(
            "username", 
            "password", 
            "server/host", 
            "database", 
            ""INSERT INTO your_table (column1, column2) VALUES ('value1', 'value2')").await?;
    }

### Print information on a MYSQL/ MSSQL Server

    use rgwml::db_utils::DbConnect;

    // Print MSSQL Server Information
    DbConnect::print_mssql_databases("username", "password", "server", "default_database");
    DbConnect::print_mssql_schemas("username", "password", "server", "in_focus_database");
    DbConnect::print_mssql_tables("username", "password", "server", "in_focus_database", "schema");
    DbConnect::print_mssql_table_description("username", "password", "server", "in_focus_database", "table_name");
    DbConnect::print_mssql_architecture("username", "password", "server", "default_database");

    // Print MySQL Server Information
    DbConnect::print_mysql_databases("username", "password", "server", "default_database");
    DbConnect::print_mysql_tables("username", "password", "server", "in_focus_database");
    DbConnect::print_mysql_table_description("username", "password", "server", "in_focus_database", "table_name");
    DbConnect::print_mysql_architecture("username", "password", "server", "default_database");

    // Print Clickhouse Server Information
    DbConnect::print_clickhouse_databases("username", "password", "server");
    DbConnect::print_clickhouse_tables("username", "password", "server", "in_focus_database");
    DbConnect::print_clickhouse_table_description("username", "password", "server", "in_focus_database", "table_name");
    DbConnect::print_clickhouse_architecture("username", "password", "server");

    // Print BigQuery Server Information
    DbConnect::print_google_big_query_datasets("path/to/your/json/credentials", "your_project_id");
    DbConnect::print_google_big_query_tables("path/to/your/json/credentials", "your_project_id", "dataset_name");
    DbConnect::print_google_big_query_table_description("path/to/your/json/credentials", "your_project_id", "dataset_name", "table_name");
    DbConnect::print_google_big_query_architecture("path/to/your/json/credentials", "your_project_id"); // Note: Your json credentials must have READ METADATA access for this to work

6. `dc_utils`
-------------

Get dataset/ sheet name information from Data Container storage types.

### 6.1. Extract data from H5 Files

    use rgwml::csv_utils::CsvBuilder;
    use rgwml::dc_utils::{DcConnectConfig, DataContainer};
    use tokio::runtime::Runtime;
    use std::path::PathBuf;
    
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
    
        let dc_connect_config = DcConnectConfig {
            path: "path/to/your/h5/file".to_string(),
            dc_type: "H5".to_string(),
            h5_dataset_identifier: "random_data".to_string(), // Name of the data set
            h5_identifier_type: "DATASET_NAME".to_string(),   // If the h5_dataset_identifier is the dataset index, set this to DATASET_ID
        };
    
        let result = DataContainer::get_dc_data(dc_connect_config).await;
    });

### 6.2. Get Sheet Names in XLS Files

    use rgwml::dc_utils::DataContainer;
    use std::env;
    use std::path::PathBuf;
    
    let test_file_path_str = "path/to/your/file";
    let sheet_names = DataContainer::get_xls_sheet_names(test_file_path_str);
    let names = sheet_names.unwrap();

### 6.3. Get Sheet Names in XLSX Files

    use rgwml::dc_utils::DataContainer;
    use std::env;
    use std::path::PathBuf;
    
    let test_file_path_str = "path/to/your/file";
    let sheet_names = DataContainer::get_xlsx_sheet_names(test_file_path_str);
    let names = sheet_names.unwrap();

### 6.4. Get Data Set names in H5 Files

    use rgwml::dc_utils::DataContainer;
    use std::env;
    use std::path::PathBuf;
    
    let test_file_path_str = "path/to/your/file";
    let sheet_names = DataContainer::get_h5_dataset_names(test_file_path_str);
    let names = sheet_names.unwrap();

7. `xgb_utils`
--------------

A python-dependant toolkit for interacting with the XGBoost API, that helps you create XGBoost models, extract details of XGBoost models, and invoke XGBoost models for predictions.

### 7.1. Create Xgb Models

    use rgwml::csv_utils::CsvBuilder;
    use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    use tokio::runtime::Runtime;
    use std::path::PathBuf;
    
    let current_dir = std::env::current_dir().unwrap();
    let rt = Runtime::new().unwrap();
    
    rt.block_on(async {
    
        // Append the file name to the directory path
        let csv_path = current_dir.join("test_file_samples/xgb_test_files/xgb_regression_training_sample.csv");
        
        // Convert the path to a string
        let csv_path_str = csv_path.to_str().unwrap();
        // Append the relative path to the current directory
        let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models");
        
        // Convert the path to a string
        let model_dir_str = model_dir.to_str().unwrap();
        let param_column_names = "no_of_tickets, last_60_days_tickets";
        let target_column_name = "churn_day";
        let prediction_column_name = "churn_day_PREDICTIONS";
        let model_name_str = "test_reg_model";
        
        let xgb_config = XgbConfig {
            objective: "reg:squarederror".to_string(),
            max_depth: "6".to_string(),
            learning_rate: "0.05".to_string(),
            n_estimators: "200".to_string(),
            gamma: "0.2".to_string(),
            min_child_weight: "5".to_string(),
            subsample: "0.8".to_string(),
            colsample_bytree: "0.8".to_string(),
            reg_lambda: "2.0".to_string(),
            reg_alpha: "0.5".to_string(),
            scale_pos_weight: "".to_string(),
            max_delta_step: "".to_string(),
            booster: "".to_string(),
            tree_method: "".to_string(),
            grow_policy: "".to_string(),
            eval_metric: "".to_string(),
            early_stopping_rounds: "".to_string(),
            device: "".to_string(),
            cv: "".to_string(),
            interaction_constraints: "".to_string(),
        };

        let result = XgbConnect::train(csv_path_str, param_column_names, target_column_name, prediction_column_name, model_dir_str, model_name_str, xgb_config).await;

    });
    
### 7.2. Extract Details from Xgb Models

    use rgwml::csv_utils::CsvBuilder;
    use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    use std::path::PathBuf;
    
    // Get the current working directory
    let current_dir = std::env::current_dir().unwrap();
    
    // Append the relative path to the current directory
    let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models");
    
    // Convert the path to a string
    let models_path = model_dir.to_str().unwrap();

    let mut csv_builder = XgbConnect::get_all_xgb_models(models_path).expect("Failed to load XGB models");

### 7.3. Invoke XgbModels

    use rgwml::csv_utils::CsvBuilder;
    use rgwml::xgb_utils::{XgbConfig, XgbConnect};
    use tokio::runtime::Runtime;
    use std::path::PathBuf;
    
    // Get the current working directory
    let current_dir = std::env::current_dir().unwrap();
    
    // Append the relative path to the current directory
    let model_dir = current_dir.join("test_file_samples/xgb_test_files/xgb_test_models");

    // Append the file name to the directory path
    let model_path = model_dir.join("test_reg_model.json");
    
    // Convert the path to a string
    let model_path_str = model_path.to_str().unwrap();
    
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let headers = vec!["category".to_string(), "name".to_string(), "age".to_string(), "date".to_string(), "flag".to_string()];
        let data = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string(), "2023-01-01 12:00:00".to_string(), "1".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "22".to_string(), "2022-12-31 11:59:59".to_string(), "0".to_string()],
            vec!["1".to_string(), "Charlie".to_string(), "25".to_string(), "2023-01-02 13:00:00".to_string(), "1".to_string()],
            vec!["1".to_string(), "Charlie".to_string(), "28".to_string(), "2023-01-01 11:00:00".to_string(), "0".to_string()]
        ];
    
        let mut builder = CsvBuilder::from_raw_data(headers, data);

        // Append the relative path to the current directory
        let csv_path = current_dir.join("test_file_samples/xgb_test_files/xgb_regression_training_sample.csv");
        let csv_path_str = csv_path.to_str().unwrap();
        let param_column_names = "no_of_tickets,last_60_days_tickets";
        let model_path_str = "/home/rgw/Desktop/csv_db/xgb_models/test_reg_model.json";
        let prediction_column_name = "churn_day_PREDICTION";
    
        let result = XgbConnect::predict(csv_path_str, param_column_names, prediction_column_name, model_path_str).await;
    });

8. `clustering_utils`
---------------------

A python-dependant toolkit for interacting with the scikit-learn API, that helps you append a clustering column to a CSV file based on classic clustering alogrithms such as `KMEANS, DBSCAN, AGGLOMERATIVE, MEAN_SHIFT, GMM, SPECTRAL, BIRCH`. The API is flexible enough to streamline situations where the ideal number of n clusterns can be algorithmically determined by `ELBOW and SILHOUETTE` techniques.

    use rgwml::csv_utils::CsvBuilder;
    use rgwml::clustering_utils::{ClusteringConfig, ClusteringConnect};
    use tokio::runtime::Runtime;
    use std::path::PathBuf;
    
    
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
    
        let csv_path_str = "path/to/your/csv/file";
        let param_column_names = "age,annual_income,spending_score";
        let cluster_column_name = "CLUSTERS";
    
        let clustering_config = ClusteringConfig {
            operation: "KMEANS".to_string(),        // 'KMEANS', 'DBSCAN', 'AGGLOMERATIVE', 'MEAN_SHIFT', 'GMM', 'SPECTRAL', 'BIRCH'
            optimal_n_cluster_finding_method: "ELBOW".to_string(),  //  Options: FIXED:{n}, ELBOW, SILHOUETTE; Not relevant for DBSCAN and MEAN_SHIFT
            dbscan_eps: "".to_string(),             // Only relevant for DBSCAN
            dbscan_min_samples: "".to_string()      // Only relevant for DBSCAN
        };
    
        let result = ClusteringConnect::cluster(csv_path_str, param_column_names, cluster_column_name, clustering_config).await;
        dbg!(&result);
    });

9. `ai_utils`
-----------

This library provides simple AI utilities for neural association analysis, as well as connecting with the OpenAI JSON mode and BATCH processing API. 

### 9.1. Rust Native AI Functionalities

It focuses on using simple Levenshtein/ Fuzzy matching for processing and analyzing data within neural networks, with an emphasis on understanding AI decision-making processes and text analysis, optimized for a parallel computing environment.

    use rgwml::ai_utils::{fuzzai, SplitUpto, ShowComplications, WordLengthSensitivity};
    use std::error::Error;

    #[tokio::main]
    async fn main() {
        // Call the fuzzai function with CSV file path
        let fuzzai_result = fuzzai(
            "path/to/your/model/training/csv/file.csv",
            "model_training_input_column_name",
            "model_training_output_column_name",
            "your text to be analyzed against the training data model",
            "your task description: clustering customer complaints",
            SplitUpto::WordSetLength(2), // Set the minimum word length of combination value to split the training input data during the analysis
            ShowComplications::False, // Set to True to see inner workings of the model
            WordLengthSensitivity::Coefficient(0.2), // Set to Coefficient::None to disregard differences in the word length of the training input and the text being analyzed; Increase the coefficient to give higher weightage to matches with similar word length
        ).await.expect("Analysis should succeed");

        dbg!(fuzzai_result);
    }

### 9.2. OpenAI API Functionalities

#### 9.2.1. OpenAI Synchronus JSON mode

    use rgwml::ai_utils::{get_openai_analysis_json};
    use std::collections::HashMap;

    let customer_feedback = "Your servcies are great!";
    let mut analysis_query = HashMap::new();
    analysis_query.insert("was_positive".to_string(), "Return true if the sentiment is positive, else return False".to_string());

    let analysis = get_openai_analysis_json(
        customer_feedback,
        analysis_query,
        "your/OpenAI/API/key"
        "gpt-3.5-turbo" // Or any model supporting JSON Mode
    );

    dbg!(analysis); 

#### 9.2.2. OpenAI Asynchronus BATCH mode

    use rgwml::ai_utils::{upload_file_to_openai, create_openai_batch, fetch_and_print_openai_batches, cancel_openai_batch};
    use rgwml::csv_utils::CsvBuilder;
    use std::collections::HashMap;


    let headers = vec!["customer_feedback".to_string(), "resolution_time".to_string()];
    let data = vec![
        vec!["Your services are great!".to_string(), "5".to_string()],
        vec!["Not satisfied with the resolution.".to_string(), "15".to_string()],
    ];

    let mut csv_builder = CsvBuilder::from_raw_data(headers, data);

    let columns_to_analyze = vec!["customer_feedback", "resolution_time"];
    let mut analysis_query = HashMap::new();
    analysis_query.insert("was_positive".to_string(), "Return true if the sentiment is positive, else return False".to_string());
    let api_key = "your_openai_api_key";
    let model = "gpt-3.5-turbo";
    let batch_description = "Positive Sentiment Analysis";

    // Send OpenAI a batch task
    let batch_id = csv_builder.send_data_for_openai_batch_analysis(
        columns_to_analyze,
        analysis_query,
        &api_key,
        model,
        batch_description
    ).await?;

    dbg!(&batch_id);

    // To fetch and print details of all your batch tasks
    let _ = fetch_and_print_openai_batches(api_key).await?;

    // To cancel the batch task
    let _ = cancel_openai_batch(api_key, batch_id).await?;

    // To retreive an OpenAI batch analysiss as a named temp file `Result<NamedTempFile, Box<dyn Error>>`
    let _ = retrieve_openai_batch(api_key, file_id)

10. `public_url_utils`
---------------------

Provides simple utilities to retreive data from popular publicly available interfaces such as a publicly viewable Google Sheet.

    use rgwml::csv_utils::CsvBuilder;
    use rgwml::public_url_utils::{PublicUrlConnectConfig, PublicUrlConnect};
    use tokio::runtime::Runtime;
    use std::path::PathBuf;
    
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
    
        let public_url_connect_config = PublicUrlConnectConfig {
        url: "https://docs.google.com/spreadsheets/d/1U9ozNFwV__c15z4Mp_EWorGwOv6mZPaQ9dmYtjmCPow/edit#gid=272498272".to_string(),
        url_type: "GOOGLE_SHEETS".to_string(),
        };
        
        let result = PublicUrlConnect::get_google_sheets_data(public_url_connect_config).await;

    });

11. `api_utils`
------------

Examples across common API call patterns

    use serde_json::json;
    use rgwml::api_utils::ApiCallBuilder;
    use std::collections::HashMap;

    #[tokio::main]
    async fn main() {
        // Fetch and cache post request without headers, with retry mechanism
        let response = fetch_and_cache_post_request().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch data: {}", e);
            std::process::exit(1);
        });
        println!("Response: {:?}", response);

        // Fetch and cache post request with headers, with retry mechanism
        let response_with_headers = fetch_and_cache_post_request_with_headers().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch data with headers: {}", e);
            std::process::exit(1);
        });
        println!("Response with headers: {:?}", response_with_headers);

        // Fetch and cache post request with form URL encoded content type, with retry mechanism
        let response_form_urlencoded = fetch_and_cache_post_request_form_urlencoded().await.unwrap_or_else(|e| {
            eprintln!("Failed to fetch form URL encoded data: {}", e);
            std::process::exit(1);
        });
        println!("Form URL encoded response: {:?}", response_form_urlencoded);
    }

    // Example 1: Without Headers, includes retry mechanism
    async fn fetch_and_cache_post_request() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let payload = json!({
            "field1": "Hello",
            "field2": 123
        });

        let response = ApiCallBuilder::call(method, url, None, Some(payload))
            .maintain_cache(30, "/path/to/post_cache.json") // Uses cache for 30 minutes
            .retries(3, 5) // Retry up to 3 times with a 5-second timeout between retries
            .execute()
            .await?;

        Ok(response)
    }

    // Example 2: With Headers, includes retry mechanism
    async fn fetch_and_cache_post_request_with_headers() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let headers = json!({
            "Content-Type": "application/json",
            "Authorization": "Bearer your_token_here"
        });
        let payload = json!({
            "field1": "Hello",
            "field2": 123
        });

        let response = ApiCallBuilder::call(method, url, Some(headers), Some(payload))
            .maintain_cache(30, "/path/to/post_with_headers_cache.json") // Uses cache for 30 minutes
            .retries(3, 5) // Retry up to 3 times with a 5-second timeout between retries
            .execute()
            .await?;

        Ok(response)
    }

    // Example 3: With application/x-www-form-urlencoded Content-Type, includes retry mechanism
    async fn fetch_and_cache_post_request_form_urlencoded() -> Result<String, Box<dyn std::error::Error>> {
        let method = "POST";
        let url = "http://example.com/api/submit";
        let headers = json!({
            "Content-Type": "application/x-www-form-urlencoded"
        });
        let payload = HashMap::from([
            ("field1", "value1"),
            ("field2", "value2"),
        ]);

        let response = ApiCallBuilder::call(method, url, Some(headers), Some(payload))
            .maintain_cache(30, "/path/to/post_form_urlencoded_cache.json") // Uses cache for 30 minutes
            .retries(3, 5) // Retry up to 3 times with a 5-second timeout between retries
            .execute()
            .await?;

        Ok(response)
    }


