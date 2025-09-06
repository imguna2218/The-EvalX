use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use priority_queue::PriorityQueue;

pub mod task;
pub mod manager;

pub use task::{ExecutionTask, ExecutionType};
pub use manager::QueueManager;

#[derive(Debug, Clone)]
pub struct ExecutionQueue {
    inner: Arc<Mutex<HashMap<String, PriorityQueue<String, u8>>>>,
    notify: Arc<Notify>,
}

impl ExecutionQueue {
    pub fn new() -> Self {
        ExecutionQueue {
            inner: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn enqueue(&self, task_id: String, language: &str, version: &str, priority: u8) {
        let key = format!("{}:{}", language, version);
        let mut queues = self.inner.lock().await;
        let queue = queues.entry(key).or_insert_with(PriorityQueue::new);
        queue.push(task_id, priority);
        self.notify.notify_one();
    }

    pub async fn dequeue(&self, language: &str, version: &str) -> Option<String> {
        let key = format!("{}:{}", language, version);
        loop {
            let mut queues = self.inner.lock().await;
            if let Some(queue) = queues.get_mut(&key) {
                if let Some((task_id, _)) = queue.pop() {
                    return Some(task_id);
                }
            }
            drop(queues);
            self.notify.notified().await;
        }
    }

    pub async fn remove(&self, task_id: &str, language: &str, version: &str) {
        let key = format!("{}:{}", language, version);
        let mut queues = self.inner.lock().await;
        if let Some(queue) = queues.get_mut(&key) {
            queue.remove(task_id);
        }
    }
}