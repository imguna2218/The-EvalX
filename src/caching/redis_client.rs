use redis::{Client, Commands, AsyncCommands};
use serde::{Serialize, Deserialize};
use std::env;
use anyhow::Result;
use sha2::{Sha256, Digest};
use hex::ToHex;

#[derive(Debug, Clone)]
pub struct RedisClient {
    client: Client,
}

impl RedisClient {
    pub fn new() -> Result<Self> {
        let redis_host = env::var("REDIS_HOST").unwrap_or_else(|_| "localhost".to_string());
        let redis_port = env::var("REDIS_PORT").unwrap_or_else(|_| "6379".to_string());
        let redis_url = format!("redis://{}:{}", redis_host, redis_port);
        let client = Client::open(redis_url)?;
        Ok(RedisClient { client })
    }

    pub async fn get_async_connection(&self) -> Result<redis::aio::Connection> {
        self.client.get_async_connection().await.map_err(|e| anyhow::anyhow!("Failed to get async Redis connection: {}", e))
    }

    pub fn get_from_cache<T: for<'de> Deserialize<'de>>(&mut self, key: &str) -> Result<Option<T>> {
        let mut conn = self.client.get_connection()?;
        let value: Option<String> = conn.get(key)?;
        match value {
            Some(val) => Ok(Some(serde_json::from_str(&val)?)),
            None => Ok(None),
        }
    }

    pub fn set_in_cache<T: Serialize>(&mut self, key: &str, value: &T, ttl_seconds: usize) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let serialized = serde_json::to_string(value)?;
        conn.set_ex::<_, _, ()>(key, serialized, ttl_seconds)?;
        Ok(())
    }

    pub fn get_binary(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.client.get_connection()?;
        let value: Option<Vec<u8>> = conn.get(key)?;
        Ok(value)
    }

    pub fn set_binary(&mut self, key: &str, value: &[u8], ttl_seconds: usize) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        conn.set_ex::<_, _, ()>(key, value, ttl_seconds)?;
        Ok(())
    }

    pub fn get_artifact(&mut self, key: &str) -> Result<Option<crate::models::response::Artifact>> {
        let mut conn = self.client.get_connection()?;
        let value: Option<String> = conn.get(key)?;
        match value {
            Some(val) => Ok(Some(serde_json::from_str(&val)?)),
            None => Ok(None),
        }
    }

    pub fn set_artifact(&mut self, key: &str, code: &str, binary: &[u8], ttl_seconds: usize) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let artifact = crate::models::response::Artifact {
            code: code.to_string(),
            binary: binary.to_vec(),
        };
        let serialized = serde_json::to_string(&artifact)?;
        conn.set_ex::<_, _, ()>(key, serialized, ttl_seconds)?;
        Ok(())
    }

    // New: Batch-level caching methods
    pub fn get_batch_from_cache(&mut self, code_hash: &str, test_hashes: &[String]) -> Result<Option<Vec<crate::models::response::EvaluationResult>>> {
        let mut sorted_hashes = test_hashes.to_vec();
        sorted_hashes.sort();
        let batch_key = format!("evalx:batch:{}", Sha256::digest(sorted_hashes.join(":").as_bytes()).encode_hex::<String>());
        let full_key = format!("{}:{}", code_hash, batch_key);
        self.get_from_cache(&full_key)
    }

    pub fn set_batch_in_cache(&mut self, code_hash: &str, test_hashes: &[String], results: &Vec<crate::models::response::EvaluationResult>, ttl_seconds: usize) -> Result<()> {
        let mut sorted_hashes = test_hashes.to_vec();
        sorted_hashes.sort();
        let batch_key = format!("evalx:batch:{}", Sha256::digest(sorted_hashes.join(":").as_bytes()).encode_hex::<String>());
        let full_key = format!("{}:{}", code_hash, batch_key);
        self.set_in_cache(&full_key, results, ttl_seconds)
    }
}