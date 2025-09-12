use redis::{Client, Commands, AsyncCommands};
use serde::{Serialize, Deserialize};
use std::env;
use anyhow::Result;
use sha2::{Sha256, Digest};
use hex::ToHex;
use crate::models::response::{Artifact, EvaluationResult};

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
            Some(val) => {
                // Track cache hit
                let _: () = conn.incr::<_, _, ()>("evalx:cache:hits", 1)?;
                Ok(Some(serde_json::from_str(&val)?))
            },
            None => {
                // Track cache miss
                let _: () = conn.incr::<_, _, ()>("evalx:cache:misses", 1)?;
                Ok(None)
            },
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

    pub fn get_artifact(&mut self, key: &str) -> Result<Option<Artifact>> {
        let mut conn = self.client.get_connection()?;
        let value: Option<String> = conn.get(key)?;
        match value {
            Some(val) => {
                let _: () = conn.incr::<_, _, ()>("evalx:cache:hits", 1)?;
                Ok(Some(serde_json::from_str(&val)?))
            },
            None => {
                let _: () = conn.incr::<_, _, ()>("evalx:cache:misses", 1)?;
                Ok(None)
            },
        }
    }

    pub fn set_artifact(&mut self, key: &str, code: &str, binary: &[u8], ttl_seconds: usize) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let artifact = Artifact {
            code: code.to_string(),
            binary: binary.to_vec(),
        };
        let serialized = serde_json::to_string(&artifact)?;
        conn.set_ex::<_, _, ()>(key, serialized, ttl_seconds)?;
        Ok(())
    }

    // Batch-level caching methods
    pub fn get_batch_from_cache(&mut self, code_hash: &str, test_hashes: &[String]) -> Result<Option<Vec<EvaluationResult>>> {
        let mut conn = self.client.get_connection()?;
        let batch_key = self.generate_batch_key(test_hashes);
        let full_key = format!("evalx:batch:{}:{}", code_hash, batch_key);
        
        let value: Option<String> = conn.get(&full_key)?;
        match value {
            Some(val) => {
                let _: () = conn.incr::<_, _, ()>("evalx:cache:hits", 1)?;
                Ok(Some(serde_json::from_str(&val)?))
            },
            None => {
                let _: () = conn.incr::<_, _, ()>("evalx:cache:misses", 1)?;
                Ok(None)
            },
        }
    }

    pub fn set_batch_in_cache(&mut self, code_hash: &str, test_hashes: &[String], results: &Vec<EvaluationResult>, ttl_seconds: usize) -> Result<()> {
        let mut conn = self.client.get_connection()?;
        let batch_key = self.generate_batch_key(test_hashes);
        let full_key = format!("evalx:batch:{}:{}", code_hash, batch_key);
        let serialized = serde_json::to_string(results)?;
        conn.set_ex::<_, _, ()>(&full_key, serialized, ttl_seconds)?;
        Ok(())
    }

    fn generate_batch_key(&self, test_hashes: &[String]) -> String {
        let mut sorted_hashes = test_hashes.to_vec();
        sorted_hashes.sort();
        Sha256::digest(sorted_hashes.join(":").as_bytes()).encode_hex::<String>()
    }

    // Cache statistics methods
    pub fn get_cache_stats(&mut self) -> Result<(u64, u64)> {
        let mut conn = self.client.get_connection()?;
        let hits: u64 = conn.get("evalx:cache:hits").unwrap_or(0);
        let misses: u64 = conn.get("evalx:cache:misses").unwrap_or(0);
        Ok((hits, misses))
    }

    // Partial cache utilization - check if compilation artifact exists
    pub fn get_compilation_artifact(&mut self, language: &str, version: &str, code: &str) -> Result<Option<Artifact>> {
        let code_hash = Sha256::digest(code).encode_hex::<String>();
        let artifact_key = format!("evalx:artifact:{}:{}:{}", language, version, code_hash);
        self.get_artifact(&artifact_key)
    }

    // Async methods for hit/miss tracking (Phase 1 Step 4)
    pub async fn increment_cache_hit(&self) -> Result<()> {
        let mut conn = self.get_async_connection().await?;
        conn.incr::<_, _, ()>("cache:hits", 1).await?;
        Ok(())
    }

    pub async fn increment_cache_miss(&self) -> Result<()> {
        let mut conn = self.get_async_connection().await?;
        conn.incr::<_, _, ()>("cache:misses", 1).await?;
        Ok(())
    }
}