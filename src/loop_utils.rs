use futures::future::join_all;
use std::future::Future;

pub struct FutureLoop;

impl FutureLoop {

        pub fn get_docs() -> String {
        let docs = r#"

++++++++++++++++
+> FutureLoop <+
++++++++++++++++

1. FutureLoops::future_for
-----------------------
        
    let data = vec![1, 2, 3, 4, 5];
    let results = FutureLoop::future_for(data, |num| async move {
        // Perform async operation
        num * 2
    }).await;
    println!("Results: {:?}", results);
   

"#;
        // docs.to_string();

        println!("{}", docs.to_string());

        docs.to_string()
    }

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


