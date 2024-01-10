use futures::future::join_all;
use std::future::Future;

pub struct FutureLoop;

impl FutureLoop {

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
