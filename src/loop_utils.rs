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

This function suits scenarios where you need a moderate level of concurrency. For instance, when each item's processing is relatively straightforward and doesn't require complex shared state or high-throughput parallelism.

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
}
