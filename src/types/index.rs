use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Semaphore, Mutex, RwLock};
use std::time::Instant;
use sysinfo::System;
use crate::caching::redis_client::RedisClient;
use crate::models::response::EvaluationResult;
use crate::languages::manager::LanguageRegistry;


/// ADDED: Represents the two operational states for concurrency control.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConcurrencyState {
    Nominal,
    Strained,
}


pub struct CodeExecutor {
    pub semaphore: Arc<Semaphore>,
    pub system: Arc<Mutex<System>>,
    pub redis_client: RedisClient,
    pub concurrency_state: Arc<RwLock<(ConcurrencyState, Instant)>>,
    pub language_registry: Arc<LanguageRegistry>,
}

impl std::fmt::Debug for CodeExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodeExecutor")
            .field("semaphore", &self.semaphore)
            .finish_non_exhaustive()
    }
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionNotification {
    pub id: String,
    pub status: String,
    pub results: Option<Vec<EvaluationResult>>,
}