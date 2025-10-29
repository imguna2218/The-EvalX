use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

const LOCAL_POOL_SIZE: u16 = 25; // Or read from env var

#[derive(Clone, Debug)]
pub struct LocalSandboxPool {
    ready_rx: Arc<Mutex<mpsc::Receiver<u16>>>,
    cleanup_tx: mpsc::Sender<u16>,
}

impl LocalSandboxPool {
    pub async fn new() -> Result<Self> {
        // Create channels with buffer size matching the pool size
        let (ready_tx, ready_rx) = mpsc::channel(LOCAL_POOL_SIZE as usize);
        let (cleanup_tx, cleanup_rx) = mpsc::channel(LOCAL_POOL_SIZE as usize);

        // Spawn the local recycler task
        tokio::spawn(Self::run_recycler(cleanup_rx, ready_tx.clone()));

        // Initialize boxes and fill the ready channel
        info!(
            "Initializing {} local sandbox boxes (0 to {})...",
            LOCAL_POOL_SIZE,
            LOCAL_POOL_SIZE - 1
        );
        let mut initialized_count = 0;
        for i in 0..LOCAL_POOL_SIZE {
            // Cleanup first, just in case
            let _ = Command::new("isolate")
                .arg("--cg")
                .arg(format!("--box-id={}", i))
                .arg("--cleanup")
                .status()
                .await;

            // Initialize
            let init_status = Command::new("isolate")
                .arg("--cg")
                .arg(format!("--box-id={}", i))
                .arg("--init")
                .status()
                .await;

            if init_status.is_ok() && init_status.unwrap().success() {
                ready_tx.send(i).await?;
                initialized_count += 1;
            } else {
                warn!("Failed to initialize local box #{}. It will not be available.", i);
            }
        }

        if initialized_count == 0 {
            error!("FATAL: No local sandbox boxes could be initialized.");
            return Err(anyhow!("Failed to initialize any local sandbox boxes"));
        }
        info!(
            "Local sandbox pool initialized with {} ready boxes.",
            initialized_count
        );

        Ok(Self {
            ready_rx: Arc::new(Mutex::new(ready_rx)),
            cleanup_tx,
        })
    }

    async fn run_recycler(mut cleanup_rx: mpsc::Receiver<u16>, ready_tx: mpsc::Sender<u16>) {
        info!("Starting Local Sandbox Recycler task...");
        while let Some(box_id) = cleanup_rx.recv().await {
            debug!("Local Recycler: Recycling box #{}", box_id);

            // Cleanup the used box
            let cleanup_status = Command::new("isolate")
                .arg("--cg")
                .arg(format!("--box-id={}", box_id))
                .arg("--cleanup")
                .status()
                .await;
             if !(cleanup_status.is_ok() && cleanup_status.unwrap().success()){
                 warn!("Local Recycler: Cleanup failed for box #{}, attempting re-init anyway.", box_id);
             }


            // Re-initialize it to make it pristine
            let init_status = Command::new("isolate")
                .arg("--cg")
                .arg(format!("--box-id={}", box_id))
                .arg("--init")
                .status()
                .await;

            // If successful, return it to the ready pool
            if init_status.is_ok() && init_status.unwrap().success() {
                if let Err(e) = ready_tx.send(box_id).await {
                     error!("Local Recycler: Failed to send box #{} back to ready pool: {}", box_id, e);
                     // If sending fails, the channel might be closed, so we break.
                     break;
                } else {
                    debug!("Local Recycler: Box #{} is now ready.", box_id);
                }
            } else {
                error!("Local Recycler: Failed to re-initialize box #{}. It will not be returned to the pool.", box_id);
                // Consider adding a mechanism here to eventually retry initialization or alert.
            }
        }
         error!("Local Sandbox Recycler task is terminating because the cleanup channel was closed.");
    }

    pub async fn acquire(&self) -> Result<u16> {
        let mut receiver = self.ready_rx.lock().await;
        // Wait indefinitely for a box ID. Add timeout logic if needed.
        receiver
            .recv()
            .await
            .ok_or_else(|| anyhow!("Sandbox ready channel closed unexpectedly"))
    }

    pub async fn release(&self, box_id: u16) {
        if let Err(e) = self.cleanup_tx.send(box_id).await {
            error!(
                "CRITICAL LEAK: Failed to send sandbox ID {} to local cleanup queue: {}",
                box_id, e
            );
        }
    }
}