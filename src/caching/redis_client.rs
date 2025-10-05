use anyhow::{anyhow, Result};
use mobc::{Connection, Pool};
use mobc_redis::redis::{self, aio::MultiplexedConnection, AsyncCommands};
use mobc_redis::RedisConnectionManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::models::response::Artifact;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct RedisClient {
    pool: Pool<RedisConnectionManager>,
    /// ADDED: Tracks the last access time of artifact keys for LRU eviction.
    artifact_access: Arc<Mutex<HashMap<String, Instant>>>,
}

// ADDED: Manual Debug implementation to satisfy trait bounds from other structs.
impl std::fmt::Debug for RedisClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisClient").finish_non_exhaustive()
    }
}

/// Helper function to compress binary data using GZIP.
fn compress_binary(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    encoder.finish().map_err(|e| anyhow!(e))
}

/// Helper function to decompress GZIP-compressed binary data.
fn decompress_binary(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}


impl RedisClient {
    pub async fn new() -> Result<Self> {
        let redis_host = env::var("REDIS_HOST").unwrap_or_else(|_| "localhost".to_string());
        let redis_port = env::var("REDIS_PORT").unwrap_or_else(|_| "6379".to_string());
        let redis_url = format!("redis://{}:{}", redis_host, redis_port);

        let client = redis::Client::open(redis_url)?;
        let manager = RedisConnectionManager::new(client);
        let pool = Pool::builder()
            .max_open(100) // Max 100 connections
            .max_idle(20) // Keep up to 20 idle connections ready
            .get_timeout(Some(Duration::from_secs(5))) // Wait max 5s for a connection
            .max_lifetime(Some(Duration::from_secs(3600))) // Recycle connections after 1hr
            .build(manager);
        
        Ok(RedisClient {
            pool,
            // ADDED: Initialize the artifact access tracker.
            artifact_access: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    // Helper to get a connection from the pool
    async fn get_conn(&self) -> Result<Connection<RedisConnectionManager>> {
        self.pool.get().await.map_err(|e| anyhow!("Failed to get Redis connection from pool: {}", e))
    }

    /// This is a convenience method for parts of the app (like the queue manager)
    /// that need a dedicated multiplexed connection for pub/sub or blocking operations.
    pub async fn get_multiplexed_async_connection(&self) -> Result<MultiplexedConnection> {
        // This method needs to create a new client to get a truly separate multiplexed connection,
        // which is ideal for long-lived worker tasks like BRPOP.
        let redis_host = env::var("REDIS_HOST").unwrap_or_else(|_| "localhost".to_string());
        let redis_port = env::var("REDIS_PORT").unwrap_or_else(|_| "6379".to_string());
        let redis_url = format!("redis://{}:{}", redis_host, redis_port);
        
        let client = redis::Client::open(redis_url)?;
        client.get_multiplexed_async_connection().await.map_err(|e| anyhow!("Failed to get multiplexed Redis connection: {}", e))
    }

    // --- Asynchronous Methods (Non-Blocking) ---

    pub async fn get_from_cache_async<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Result<Option<T>> {
        let mut conn = self.get_conn().await?;
        let value: Option<String> = conn.get(key).await?;
        match value {
            Some(val) => {
                let _: () = conn.incr("evalx:cache:hits", 1).await?;
                Ok(Some(serde_json::from_str(&val)?))
            },
            None => {
                let _: () = conn.incr("evalx:cache:misses", 1).await?;
                Ok(None)
            },
        }
    }

    pub async fn set_in_cache_async<T: Serialize>(&self, key: &str, value: &T, ttl_seconds: usize) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let serialized = serde_json::to_string(value)?;
        conn.set_ex::<_, _, ()>(key, serialized, ttl_seconds).await?;
        Ok(())
    }

    /// MODIFIED: Now updates the access time for the artifact on cache hit.
    pub async fn get_artifact_async(&self, key: &str) -> Result<Option<Artifact>> {
        let mut conn = self.get_conn().await?;
        let value: Option<String> = conn.get(key).await?;
        match value {
            Some(val) => {
                let _: () = conn.incr("evalx:cache:hits", 1).await?;
                let mut artifact: Artifact = serde_json::from_str(&val)?;
                
                // ADDED: Update the last-accessed time for this artifact.
                let mut access_map = self.artifact_access.lock().await;
                access_map.insert(key.to_string(), Instant::now());
                
                if artifact.compressed {
                    debug!("Decompressing artifact for key: {}", key);
                    artifact.binary = decompress_binary(&artifact.binary)?;
                    artifact.compressed = false; // Set to false after decompression
                }

                Ok(Some(artifact))
            },
            None => {
                let _: () = conn.incr("evalx:cache:misses", 1).await?;
                Ok(None)
            },
        }
    }

    /// MODIFIED: Now records the creation time of the artifact.
    pub async fn set_artifact_async(&self, key: &str, language: &str, code: &str, binary: &[u8]) -> Result<()> {
        let mut conn = self.get_conn().await?;
        
        let (binary_to_store, is_compressed) = if binary.len() > 1024 { // 1KB threshold
            debug!("Compressing artifact for key: {}", key);
            (compress_binary(binary)?, true)
        } else {
            (binary.to_vec(), false)
        };

        let artifact = Artifact {
            code: code.to_string(),
            binary: binary_to_store,
            compressed: is_compressed,
        };
        
        let ttl_seconds = match language {
            "java" | "java11" | "java21" => 300, 
            _ => 1800,
        };

        let serialized = serde_json::to_string(&artifact)?;
        conn.set_ex::<_, _, ()>(key, serialized, ttl_seconds).await?;

        // ADDED: Record the creation time for the new artifact.
        let mut access_map = self.artifact_access.lock().await;
        access_map.insert(key.to_string(), Instant::now());

        Ok(())
    }

    pub async fn get_cache_stats_async(&self) -> Result<(u64, u64)> {
        let mut conn = self.get_conn().await?;
        let hits: u64 = conn.get("evalx:cache:hits").await.unwrap_or(0);
        let misses: u64 = conn.get("evalx:cache:misses").await.unwrap_or(0);
        Ok((hits, misses))
    }

    /// ADDED: Background task to monitor Redis memory and evict old artifacts if needed.
    pub async fn monitor_redis_memory(&self) {
        info!("Starting Redis memory monitor task.");
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;

            let result: Result<()> = async {
                let mut conn = self.get_multiplexed_async_connection().await?;
                let info: String = redis::cmd("INFO").arg("memory").query_async(&mut conn).await?;

                let mut used_memory: Option<u64> = None;
                let mut max_memory: Option<u64> = None;

                for line in info.lines() {
                    if line.starts_with("used_memory:") {
                        used_memory = line.split(':').nth(1).and_then(|v| v.parse().ok());
                    }
                    if line.starts_with("maxmemory:") {
                        max_memory = line.split(':').nth(1).and_then(|v| v.parse().ok());
                    }
                }

                if let (Some(used), Some(max)) = (used_memory, max_memory) {
                    if max == 0 {
                        // Max memory is not set in redis.conf, so we can't calculate usage.
                        // We will skip eviction to avoid incorrect behavior.
                        return Ok(());
                    }

                    let usage_ratio = used as f64 / max as f64;
                    if usage_ratio > 0.8 {
                        warn!(
                            "Redis memory usage is at {:.2}%. Triggering aggressive cache eviction.",
                            usage_ratio * 100.0
                        );

                        let mut access_map = self.artifact_access.lock().await;
                        if access_map.is_empty() {
                            return Ok(());
                        }

                        let mut artifacts: Vec<_> = access_map.iter().collect();
                        artifacts.sort_by_key(|&(_, instant)| instant);

                        // Evict 10% of the oldest artifacts
                        let eviction_count = (artifacts.len() as f64 * 0.1).ceil() as usize;
                        let to_evict: Vec<_> = artifacts.iter().take(eviction_count).map(|(k, _)| k.to_string()).collect();

                        if !to_evict.is_empty() {
                            info!("Evicting {} oldest artifacts from cache.", to_evict.len());
                            let mut pipe = redis::pipe();
                            for key in &to_evict {
                                pipe.del(key);
                            }
                            pipe.query_async(&mut conn).await?;

                            // Remove from our tracking map
                            for key in &to_evict {
                                access_map.remove(key);
                            }
                        }
                    }
                }
                Ok(())
            }.await;

            if let Err(e) = result {
                error!("Error in Redis memory monitor: {}", e);
            }
        }
    }
}