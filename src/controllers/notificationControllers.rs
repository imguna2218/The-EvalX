use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tracing::{debug, warn};
use std::sync::Arc;
use crate::types::index::{CodeExecutor, ExecutionNotification};
use crate::caching::redis_client::RedisClient;
use crate::queue_management::QueueManager;

pub async fn handle_ws_upgrade(
    ws: WebSocketUpgrade,
    State((_executor, tx, _redis_client, _queue_manager)): State<(Arc<CodeExecutor>, Arc<broadcast::Sender<ExecutionNotification>>, RedisClient, Arc<QueueManager>)>,
) -> impl IntoResponse {
    let mut rx = tx.subscribe();
    ws.on_upgrade(|socket| async move {
        let (mut ws_sender, mut ws_receiver) = socket.split();

        tokio::spawn(async move {
            while let Ok(notification) = rx.recv().await {
                let message = serde_json::to_string(&notification).unwrap_or_default();
                if ws_sender.send(axum::extract::ws::Message::Text(message)).await.is_err() {
                    warn!("WebSocket send failed");
                    break;
                }
                debug!("Sent notification via WebSocket: {:?}", notification);
            }
        });

        while let Some(Ok(msg)) = ws_receiver.next().await {
            debug!("Received WebSocket message: {:?}", msg);
        }
    })
}