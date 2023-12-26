use futures::future::join_all;
use std::future::Future;
use std::time::Instant;

pub struct FutureLoop;

impl FutureLoop {
    pub fn get_docs() -> String {
        let docs = r#"

++++++++++++++++
+> FutureLoop <+
++++++++++++++++

1. FutureLoop::future_for
-------------------------

This function suits scenarios where you need a moderate level of concurrency. Since all futures are collected and then awaited together, it's important to ensure that spawning too many futures at once doesn't overwhelm the system resources. For instance, when each item's processing is relatively straightforward and doesn't require complex shared state or high-throughput parallelism.

Use in a single-threaded Tokio environment
    
    use rgwml::loop_utils::FutureLoop;

    #[tokio::main()]
    async fn main() {
        let data = vec![1, 2, 3, 4, 5];
        let results = FutureLoop::future_for(data, |num| async move {
            // Perform an async operation
            num * 2
        }).await;
        println!("Results: {:?}", results);
    }

Use in a multi-threaded Tokio environment:

    use rgwml::loop_utils::FutureLoop;

    #[tokio::main(flavor = "multi_thread")]
    async fn main() {
        // Data for the first task
        let data1 = vec![1, 2, 3, 4, 5];

        // Spawning the first `future_for` task
        let handle1 = tokio::spawn(async move {
            FutureLoop::future_for(data1, |num| async move {
                // Perform an async operation
                num * 2
            }).await
        });

        // Data for the second task
        let data2 = vec![6, 7, 8, 9, 10];

        // Spawning the second `future_for` task
        let handle2 = tokio::spawn(async move {
            FutureLoop::future_for(data2, |num| async move {
                // Another async operation
                num * 3
            }).await
        });

        // Awaiting results from both tasks
        let results1 = handle1.await.expect("Task 1 failed");
        let results2 = handle2.await.expect("Task 2 failed");

        println!("Results from first task: {:?}", results1);
        println!("Results from second task: {:?}", results2);
    }

Multi Threaded CPU Capacity Test

    use rgwml::loop_utils::FutureLoop;

    #[tokio::main(flavor = "multi_thread")]
    async fn main() {

        // Start the first test_capacity task and hold its future
        let handle1 = tokio::spawn(async {
            FutureLoop::test_capacity(2500).await;
        });

        // Start the second test_capacity task with a value of 1000
        let handle2 = tokio::spawn(async {
            FutureLoop::test_capacity(2500).await;
        });

        // You can await the futures here if you need to get the results or ensure completion
        let _ = handle1.await.expect("Task 1 failed");
        let _ = handle2.await.expect("Task 2 failed");

    }


"#;
        // docs.to_string();

        println!("{}", docs.to_string());

        docs.to_string()
    }

    /// Asynchronously processes an iterable, applying a provided future-returning closure to each item and returning their collective results.    
    pub async fn future_for<I, T, F, Fut>(iterable: I, mut f: F) -> Vec<T>
    where
        I: IntoIterator,
        F: FnMut(I::Item) -> Fut,
        Fut: Future<Output = T>,
    {
        let mut futures = Vec::new();
        for item in iterable {
            futures.push(f(item));
        }
        join_all(futures).await
    }
    
    /// Tests the system's capacity to handle intensive computational tasks asynchronously by dividing a range into chunks and processing them concurrently.
    pub async fn test_capacity(intensity: u64) -> u64 {
        async fn compute_intensive_task(start: u64, end: u64) -> u64 {
            // Introduce a delay before starting the computation
            //time::sleep(time::Duration::from_secs(3)).await;

            // Computation task
            let mut sum = 0;
            for i in start..end {
                for j in 0..i {
                    for k in 0..j {
                        sum += i * j * k;
                    }
                }
            }
            sum
        }

        let total_range = 0..intensity;
        let chunk_size = (total_range.end - total_range.start) / 4; // Assuming 4 chunks

        let start_time = Instant::now();

        // Utilizing future_for to manage the async tasks
        let results = FutureLoop::future_for(0..4, |i| {
            let start = total_range.start + i * chunk_size;
            let end = if i == 3 {
                total_range.end
            } else {
                start + chunk_size
            };
            compute_intensive_task(start, end)
        }).await;

        let total_sum: u64 = results.into_iter().sum();

        println!("Total sum: {}", total_sum);
        println!("Time taken: {:?}", start_time.elapsed());

        total_sum
    }

}
