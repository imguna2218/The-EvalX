use redis::{Client, Commands};
use serde::{Serialize, Deserialize};
use std::env;
use anyhow::Result;

#[derive(Clone)]
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
}
