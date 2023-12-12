use ndarray::ArrayView1;
use ndarray::{array, Array2};
use std::collections::HashSet;
use std::io;

fn process_data(data: &Array2<i32>, headers: &[&str]) {
    fn find_common_relationships(data: &Array2<i32>, headers: &[&str], result: i32) -> Vec<String> {
        let rows_with_result: Vec<_> = data
            .outer_iter()
            .filter(|row| *row.last().unwrap() == result)
            .collect();

        if rows_with_result.len() < 2 {
            println!("Not enough rows with result {} to compare.", result);
            return Vec::new();
        }

        let mut relationships = Vec::new();

        // Element-wise comparisons
        let n_cols = data.ncols();
        for i in 0..n_cols - 1 {
            for j in i + 1..n_cols - 1 {
                if rows_with_result.iter().all(|row| {
                    compare_elements(row[i], row[j])
                        == compare_elements(rows_with_result[0][i], rows_with_result[0][j])
                }) {
                    relationships.push(format!(
                        "{} {} {}",
                        headers[i],
                        compare_elements(rows_with_result[0][i], rows_with_result[0][j]),
                        headers[j]
                    ));
                }
            }
        }

        // Ratio trend comparisons
        for i in 0..n_cols - 1 {
            for j in 0..n_cols - 1 {
                if i != j {
                    for k in 0..n_cols - 1 {
                        for l in 0..n_cols - 1 {
                            if k != l && (i != k || j != l) {
                                let trend = compare_ratio_trends(&rows_with_result, i, j, k, l);
                                if trend != "no common trend" {
                                    relationships.push(format!(
                                        "{}:{} {} {}:{}",
                                        headers[i], headers[j], trend, headers[k], headers[l]
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        relationships
    }

    fn compare_elements(a: i32, b: i32) -> &'static str {
        match a.cmp(&b) {
            std::cmp::Ordering::Less => "<",
            std::cmp::Ordering::Greater => ">",
            _ => "=",
        }
    }

    fn compare_ratio_trends(
        rows: &[ArrayView1<i32>],
        col1: usize,
        col2: usize,
        col3: usize,
        col4: usize,
    ) -> &'static str {
        let mut trend = "";
        for row in rows.iter() {
            let current_trend = if calculate_ratio(row[col1], row[col2])
                < calculate_ratio(row[col3], row[col4])
            {
                "<"
            } else if calculate_ratio(row[col1], row[col2]) > calculate_ratio(row[col3], row[col4])
            {
                ">"
            } else {
                "="
            };

            if trend.is_empty() {
                trend = current_trend;
            } else if trend != current_trend {
                return "no common trend";
            }
        }
        trend
    }

    fn calculate_ratio(a: i32, b: i32) -> f32 {
        a as f32 / b as f32
    }

    /*
    let headers = ["p1", "p2", "p3", "p4", "r"];
    let data: Array2<i32> = array![
        [10, 15, 20, 25, 1],
        [11, 16, 21, 26, 1],
        [16, 12, 24, 18, 2]
    ];
    */

    // dbg!(&headers, &data);

    let result_column_index = data.ncols() - 1;
    let results = data.column(result_column_index).to_owned();
    let unique_results: HashSet<_> = results.iter().copied().collect();

    let mut super_array: Vec<Vec<String>> = Vec::new();

    for &result in unique_results.iter() {
        // println!("\n\nCommon relationships for result {}:", result);
        let relationships = find_common_relationships(&data, &headers, result);
        super_array.push(relationships);
    }

    // Print the contents of super_array

    let mut total_relationships_count = 0;
    let mut total_results_count = 0;

    for (index, relationships) in super_array.iter().enumerate() {
        let result_count = relationships.len();
        total_relationships_count += result_count;
        total_results_count += 1;

        println!(
            "Relationships for result {}: {}",
            unique_results.iter().nth(index).unwrap(),
            result_count
        );

        println!(
            "Relationships for result {}: {:?}\n",
            unique_results.iter().nth(index).unwrap(),
            relationships
        );
    }

    // Print the total counts at the end
    println!("\nTotal Relationships Count: {}", total_relationships_count);
    println!("Total Results Count: {}\n", total_results_count);
}

fn main() {
    // Ask for the number of parameters
    println!("Enter the number of parameters:");
    let mut num_params_input = String::new();
    io::stdin().read_line(&mut num_params_input).unwrap();
    let num_params: usize = num_params_input
        .trim()
        .parse()
        .expect("Please type a number!");

    // Create headers based on the number of parameters
    let headers: Vec<String> = (1..=num_params)
        .map(|i| format!("p{}", i))
        .chain(["r".to_string()])
        .collect();
    let headers_ref: Vec<&str> = headers.iter().map(AsRef::as_ref).collect();

    let mut data_sets = Vec::new();
    println!("You will now enter data for each parameter and result.");

    loop {
        let mut data_row = Vec::new();
        for i in 1..=num_params {
            println!("Enter value for parameter{}:", i);
            let mut param_input = String::new();
            io::stdin().read_line(&mut param_input).unwrap();
            let param: i32 = param_input
                .trim()
                .parse()
                .expect("Please type a valid integer");
            data_row.push(param);
        }

        println!("Enter the result for this data set:");
        let mut result_input = String::new();
        io::stdin().read_line(&mut result_input).unwrap();
        let result: i32 = result_input
            .trim()
            .parse()
            .expect("Please type a valid integer");
        data_row.push(result);

        data_sets.push(data_row);

        // Convert the cumulative data sets into an Array2 and process it
        let data: Array2<i32> =
            Array2::from_shape_vec((data_sets.len(), num_params + 1), data_sets.concat())
                .expect("Error in forming the data array");
        process_data(&data, &headers_ref);
        /*
        println!("Enter 'y' to add another data set, or any other key to finish:");
        let mut decision = String::new();
        io::stdin().read_line(&mut decision).unwrap();
        if decision.trim().to_lowercase() != "y" {
            break;
        }
        */
    }
}
