//! Supervised Milky event WebSocket.
//!
//! The SDK's own `connect_events` spawns a read loop that simply exits when the
//! server sends a Close frame or the socket errors, and it gives the caller no
//! way to notice. A Lagrange restart therefore left the bridge "up" but deaf.
//! This module owns the event socket instead: it connects, pumps events into
//! the bridge, and reconnects with exponential backoff for as long as the
//! process runs. The SDK is still used for the (stateless) HTTP API.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use milky_rust_sdk::MilkyClient;
use milky_rust_sdk::prelude::Event as SdkEvent;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use url::Url;

use super::error::MilkyClientError;
use super::events;
use crate::types::{InboundEvent, LoginInfo};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Timing knobs for the reconnect loop.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// First delay after a disconnect or failed attempt.
    pub initial_backoff: Duration,
    /// Upper bound for the doubling delay.
    pub max_backoff: Duration,
    /// A connection that stayed up at least this long resets the backoff.
    pub stable_after: Duration,
    /// Deadline for the WebSocket handshake plus the login-info request.
    pub connect_timeout: Duration,
    /// Log at ERROR once the stream has been down for this long ...
    pub alarm_after: Duration,
    /// ... and repeat that ERROR at most this often while it stays down.
    pub alarm_every: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            stable_after: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            alarm_after: Duration::from_secs(60),
            alarm_every: Duration::from_secs(60),
        }
    }
}

/// Exponential backoff: `initial`, doubling, capped at `max`.
#[derive(Debug)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    next: Duration,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            next: initial,
        }
    }

    pub fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.max);
        delay
    }

    pub fn reset(&mut self) {
        self.next = self.initial;
    }
}

/// Build the event WebSocket URL exactly like the SDK does: the configured
/// endpoint with its path replaced by `/event` and the token as the
/// `access_token` query parameter.
pub fn event_ws_url(endpoint: &str, token: &str) -> Result<String, MilkyClientError> {
    let mut url =
        Url::parse(endpoint).map_err(|e| MilkyClientError::InvalidEndpoint(e.to_string()))?;
    url.set_path("event");
    if !token.is_empty() {
        url.query_pairs_mut().append_pair("access_token", token);
    }
    Ok(url.into())
}

pub(super) struct Supervisor {
    pub sdk: Arc<MilkyClient>,
    pub url: String,
    pub inbound_tx: mpsc::Sender<InboundEvent>,
    /// `Some(login)` while the event stream is up, `None` otherwise.
    pub state_tx: watch::Sender<Option<LoginInfo>>,
    pub shutdown_rx: watch::Receiver<bool>,
    pub policy: ReconnectPolicy,
}

enum StreamEnd {
    Shutdown,
    InboundClosed,
    Lost(String),
}

impl Supervisor {
    pub async fn run(mut self) {
        let policy = self.policy.clone();
        let mut backoff = Backoff::new(policy.initial_backoff, policy.max_backoff);
        let mut down_since = Instant::now();
        let mut last_alarm: Option<Instant> = None;
        let mut attempt: u64 = 0;
        let sdk = Arc::clone(&self.sdk);
        let url = self.url.clone();

        loop {
            if *self.shutdown_rx.borrow() {
                break;
            }
            attempt += 1;
            let connected = tokio::select! {
                res = timeout(policy.connect_timeout, connect_once(&sdk, &url)) => match res {
                    Ok(r) => r,
                    Err(_) => Err(format!(
                        "timed out after {}s",
                        policy.connect_timeout.as_secs_f32()
                    )),
                },
                _ = wait_shutdown(&mut self.shutdown_rx) => break,
            };

            match connected {
                Ok((ws, login)) => {
                    tracing::info!(
                        attempt,
                        bot_id = login.self_id,
                        "milky event stream connected"
                    );
                    self.state_tx.send_replace(Some(login.clone()));
                    let up_since = Instant::now();
                    let end = self.pump(ws, login.self_id).await;
                    self.state_tx.send_replace(None);
                    let uptime = up_since.elapsed();
                    match end {
                        StreamEnd::Shutdown => break,
                        StreamEnd::InboundClosed => {
                            tracing::warn!("inbound channel closed, stopping milky event stream");
                            break;
                        }
                        StreamEnd::Lost(reason) => tracing::warn!(
                            reason = %reason,
                            uptime_secs = uptime.as_secs(),
                            "milky event stream lost, will reconnect"
                        ),
                    }
                    if uptime >= policy.stable_after {
                        backoff.reset();
                    }
                    down_since = Instant::now();
                    last_alarm = None;
                    attempt = 0;
                }
                Err(err) => {
                    tracing::warn!(attempt, err = %err, "connect to milky event stream failed");
                }
            }

            let down_for = down_since.elapsed();
            if down_for >= policy.alarm_after
                && last_alarm.is_none_or(|t| t.elapsed() >= policy.alarm_every)
            {
                tracing::error!(
                    down_secs = down_for.as_secs(),
                    "milky event stream is still down; no events are being delivered"
                );
                last_alarm = Some(Instant::now());
            }

            let delay = backoff.next_delay();
            tracing::info!(
                delay_ms = delay.as_millis() as u64,
                "reconnecting to milky event stream"
            );
            tokio::select! {
                _ = sleep(delay) => {}
                _ = wait_shutdown(&mut self.shutdown_rx) => break,
            }
        }
        self.state_tx.send_replace(None);
        tracing::info!("milky event stream supervisor stopped");
    }

    async fn pump(&mut self, mut ws: WsStream, self_id: i64) -> StreamEnd {
        loop {
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut self.shutdown_rx) => {
                    let _ = ws.close(None).await;
                    return StreamEnd::Shutdown;
                }
                msg = ws.next() => match msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<SdkEvent>(text.as_str()) {
                            Ok(ev) => {
                                if let Some(out) = events::translate_event(ev, self_id)
                                    && self.inbound_tx.send(out).await.is_err()
                                {
                                    return StreamEnd::InboundClosed;
                                }
                            }
                            Err(e) => tracing::warn!(err = %e, "unrecognized milky event"),
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        return StreamEnd::Lost(format!("close frame: {frame:?}"));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return StreamEnd::Lost(format!("read error: {e}")),
                    None => return StreamEnd::Lost("stream ended".into()),
                },
            }
        }
    }
}

async fn connect_once(sdk: &MilkyClient, url: &str) -> Result<(WsStream, LoginInfo), String> {
    let (ws, _resp) = connect_async(url)
        .await
        .map_err(|e| format!("websocket: {e}"))?;
    // Fetch the login on every (re)connect: the first successful connect may
    // happen long after startup, and the account could change across restarts.
    let info = sdk
        .get_login_info()
        .await
        .map_err(|e| format!("get_login_info: {e}"))?;
    Ok((
        ws,
        LoginInfo {
            self_id: info.uin,
            nickname: info.nickname,
        },
    ))
}

/// Resolves once shutdown is requested (or the shutdown sender is gone).
async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|v| *v).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_caps_and_resets() {
        let mut b = Backoff::new(Duration::from_secs(1), Duration::from_secs(30));
        let got: Vec<u64> = (0..7).map(|_| b.next_delay().as_secs()).collect();
        assert_eq!(got, vec![1, 2, 4, 8, 16, 30, 30]);
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_secs(1));
    }

    #[test]
    fn event_url_matches_sdk_layout() {
        assert_eq!(
            event_ws_url("ws://lagrange:9988/event", "").unwrap(),
            "ws://lagrange:9988/event"
        );
        assert_eq!(
            event_ws_url("ws://lagrange:9988", "t k").unwrap(),
            "ws://lagrange:9988/event?access_token=t+k"
        );
        assert!(event_ws_url("not a url", "").is_err());
    }
}
