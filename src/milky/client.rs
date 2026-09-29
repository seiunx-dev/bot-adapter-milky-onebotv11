use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use milky_rust_sdk::prelude::{Event as SdkEvent, MessageScene};
use milky_rust_sdk::{Communication, MilkyClient, WebSocketConfig};
use tokio::sync::{mpsc, watch};

use super::error::MilkyClientError;
use super::events;
use super::segments;
use super::stream::{ReconnectPolicy, Supervisor, event_ws_url};
use crate::config::MilkyConfig;
use crate::types::{
    EventKind, GroupInfo, GroupMemberInfo, InboundEvent, LoginInfo, MessageRef, RequestRef, Segment,
};

pub struct Client {
    /// Used only for the HTTP API; the event socket is owned by [`Supervisor`].
    sdk: Arc<MilkyClient>,
    event_url: String,
    policy: ReconnectPolicy,
    inbound_tx: mpsc::Sender<InboundEvent>,
    state_tx: watch::Sender<Option<LoginInfo>>,
    shutdown_tx: watch::Sender<bool>,
    started: AtomicBool,
}

impl Client {
    pub fn new(
        cfg: &MilkyConfig,
        inbound_tx: mpsc::Sender<InboundEvent>,
    ) -> Result<Self, MilkyClientError> {
        Self::with_policy(cfg, inbound_tx, ReconnectPolicy::default())
    }

    pub fn with_policy(
        cfg: &MilkyConfig,
        inbound_tx: mpsc::Sender<InboundEvent>,
        policy: ReconnectPolicy,
    ) -> Result<Self, MilkyClientError> {
        let token = if cfg.token.is_empty() {
            None
        } else {
            Some(cfg.token.clone())
        };
        let ws = WebSocketConfig::new(cfg.ws_endpoint.clone(), token);
        // The SDK insists on an event sender, but we never call its
        // `connect_events` (it cannot reconnect), so nothing is ever sent here.
        let (sdk_tx, _) = mpsc::channel::<SdkEvent>(1);
        let sdk = MilkyClient::new(Communication::WebSocket(ws), sdk_tx)?;
        let event_url = event_ws_url(&cfg.ws_endpoint, &cfg.token)?;
        Ok(Self {
            sdk: Arc::new(sdk),
            event_url,
            policy,
            inbound_tx,
            state_tx: watch::channel(None).0,
            shutdown_tx: watch::channel(false).0,
            started: AtomicBool::new(false),
        })
    }

    /// Spawn the event-stream supervisor. It connects (retrying until the Milky
    /// server is reachable), forwards events to the inbound channel and
    /// reconnects with backoff whenever the stream ends. Idempotent.
    pub fn start(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let supervisor = Supervisor {
            sdk: Arc::clone(&self.sdk),
            url: self.event_url.clone(),
            inbound_tx: self.inbound_tx.clone(),
            state_tx: self.state_tx.clone(),
            shutdown_rx: self.shutdown_tx.subscribe(),
            policy: self.policy.clone(),
        };
        tokio::spawn(supervisor.run());
    }

    /// `Some(login)` while the event stream is connected, `None` otherwise.
    pub fn subscribe_state(&self) -> watch::Receiver<Option<LoginInfo>> {
        self.state_tx.subscribe()
    }

    pub async fn shutdown(&self) {
        self.shutdown_tx.send_replace(true);
    }

    pub async fn send_private_message(
        &self,
        user_id: i64,
        segments: Vec<Segment>,
    ) -> Result<i64, MilkyClientError> {
        let outgoing = segments::to_outgoing(segments)?;
        let resp = self.sdk.send_private_message(user_id, outgoing).await?;
        Ok(resp.message_seq)
    }

    pub async fn send_group_message(
        &self,
        group_id: i64,
        segments: Vec<Segment>,
    ) -> Result<i64, MilkyClientError> {
        let outgoing = segments::to_outgoing(segments)?;
        let resp = self.sdk.send_group_message(group_id, outgoing).await?;
        Ok(resp.message_seq)
    }

    pub async fn get_group_info(&self, group_id: i64) -> Result<GroupInfo, MilkyClientError> {
        let resp = self.sdk.get_group_info(group_id, true).await?;
        Ok(GroupInfo {
            group_id: resp.group.group_id,
            group_name: resp.group.group_name,
            member_count: resp.group.member_count,
            max_member_count: resp.group.max_member_count,
        })
    }

    pub async fn get_group_list(&self) -> Result<Vec<GroupInfo>, MilkyClientError> {
        let resp = self.sdk.get_group_list(true).await?;
        Ok(resp
            .groups
            .into_iter()
            .map(|g| GroupInfo {
                group_id: g.group_id,
                group_name: g.group_name,
                member_count: g.member_count,
                max_member_count: g.max_member_count,
            })
            .collect())
    }

    pub async fn get_group_member_info(
        &self,
        group_id: i64,
        user_id: i64,
    ) -> Result<GroupMemberInfo, MilkyClientError> {
        let resp = self
            .sdk
            .get_group_member_info(group_id, user_id, true)
            .await?;
        Ok(group_member_to_info(resp.member))
    }

    pub async fn get_group_member_list(
        &self,
        group_id: i64,
    ) -> Result<Vec<GroupMemberInfo>, MilkyClientError> {
        let resp = self.sdk.get_group_member_list(group_id, true).await?;
        Ok(resp.members.into_iter().map(group_member_to_info).collect())
    }

    pub async fn get_message(
        &self,
        message_ref: &MessageRef,
    ) -> Result<InboundEvent, MilkyClientError> {
        let (scene, peer_id, kind) = match message_ref.message_type.as_str() {
            "group" => (
                MessageScene::Group,
                message_ref.group_id,
                EventKind::MessageGroup,
            ),
            _ => (
                MessageScene::Friend,
                message_ref.user_id,
                EventKind::MessagePrivate,
            ),
        };
        let resp = self
            .sdk
            .get_message(scene, peer_id, message_ref.milky_seq)
            .await?;
        Ok(events::from_incoming_message(resp.message, kind))
    }

    pub async fn delete_message(&self, message_ref: &MessageRef) -> Result<(), MilkyClientError> {
        match message_ref.message_type.as_str() {
            "group" => {
                self.sdk
                    .recall_group_message(message_ref.group_id, message_ref.milky_seq)
                    .await?;
            }
            _ => {
                self.sdk
                    .recall_private_message(message_ref.user_id, message_ref.milky_seq)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn handle_friend_request(
        &self,
        request: &RequestRef,
        approve: bool,
        reason: String,
    ) -> Result<(), MilkyClientError> {
        if approve {
            self.sdk
                .accept_friend_request(request.initiator_uid, false)
                .await?;
        } else {
            self.sdk
                .reject_friend_request(request.initiator_uid, false, reason)
                .await?;
        }
        Ok(())
    }

    pub async fn handle_group_request(
        &self,
        request: &RequestRef,
        approve: bool,
    ) -> Result<(), MilkyClientError> {
        let seq = request.invitation_seq.to_string();
        if approve {
            self.sdk
                .accept_group_invitation(request.group_id, seq)
                .await?;
        } else {
            self.sdk
                .reject_group_invitation(request.group_id, seq)
                .await?;
        }
        Ok(())
    }
}

fn group_member_to_info(m: milky_rust_sdk::prelude::GroupMember) -> GroupMemberInfo {
    GroupMemberInfo {
        group_id: m.group_id,
        user_id: m.user_id,
        nickname: m.nickname,
        card: m.card,
        sex: format!("{:?}", m.sex).to_lowercase(),
        age: 0,
        area: String::new(),
        level: m.level.to_string(),
        role: format!("{:?}", m.role).to_lowercase(),
        title: m.title,
    }
}

#[cfg(test)]
mod reconnect_tests {
    //! Drives the real client against a local mock Milky server (event
    //! WebSocket + `get_login_info` HTTP API) that behaves like a restarting
    //! Lagrange: it sends a Close frame, then goes away entirely and comes back.

    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use axum::Router;
    use axum::extract::State;
    use axum::extract::ws::{Message as AxMessage, WebSocket, WebSocketUpgrade};
    use axum::response::IntoResponse;
    use axum::routing::{any, post};
    use milky_rust_sdk::prelude::{Event, EventKind as SdkKind};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::milky::stream::ReconnectPolicy;
    use crate::types::EventKind as OutboundKind;

    #[derive(Clone)]
    struct Mock {
        ws_conns: Arc<AtomicUsize>,
        logins: Arc<AtomicUsize>,
        stop: watch::Receiver<bool>,
    }

    /// Each connection `n` (1-based) sends one group-invitation event with
    /// `invitation_seq = n`. Connection 1 then sends a Close frame (the exact
    /// production symptom); later ones stay open until the server stops.
    async fn ws_handler(ws: WebSocketUpgrade, State(mock): State<Mock>) -> impl IntoResponse {
        ws.on_upgrade(move |socket| serve_socket(socket, mock))
    }

    async fn serve_socket(mut socket: WebSocket, mut mock: Mock) {
        let n = mock.ws_conns.fetch_add(1, Ordering::SeqCst) + 1;
        let ev = Event {
            time: 0,
            self_id: 10001,
            kind: SdkKind::GroupInvitation {
                group_id: 777,
                invitation_seq: n as i64,
                initiator_id: 42,
            },
        };
        let text = serde_json::to_string(&ev).unwrap();
        if socket.send(AxMessage::Text(text.into())).await.is_err() {
            return;
        }
        if n != 1 {
            let _ = mock.stop.wait_for(|v| *v).await;
        }
        let _ = socket.send(AxMessage::Close(None)).await;
    }

    async fn login_handler(State(mock): State<Mock>) -> impl IntoResponse {
        mock.logins.fetch_add(1, Ordering::SeqCst);
        axum::Json(json!({
            "status": "ok",
            "retcode": 0,
            "data": {"uin": 10001, "nickname": "mock"},
        }))
    }

    struct Server {
        stop_tx: watch::Sender<bool>,
        task: JoinHandle<()>,
    }

    impl Server {
        async fn start(
            addr: SocketAddr,
            ws_conns: Arc<AtomicUsize>,
            logins: Arc<AtomicUsize>,
        ) -> Self {
            let (stop_tx, stop_rx) = watch::channel(false);
            let mock = Mock {
                ws_conns,
                logins,
                stop: stop_rx.clone(),
            };
            let app = Router::new()
                .route("/event", any(ws_handler))
                .route("/api/get_login_info", post(login_handler))
                .with_state(mock);
            let listener = TcpListener::bind(addr).await.expect("bind mock server");
            let mut stop = stop_rx;
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        let _ = stop.wait_for(|v| *v).await;
                    })
                    .await;
            });
            Self { stop_tx, task }
        }

        async fn stop(self) {
            self.stop_tx.send_replace(true);
            if tokio::time::timeout(Duration::from_secs(2), &mut { self.task })
                .await
                .is_err()
            {
                panic!("mock server did not stop");
            }
        }
    }

    async fn next_seq(rx: &mut mpsc::Receiver<InboundEvent>) -> i64 {
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for an event")
            .expect("inbound channel closed");
        assert_eq!(ev.kind, OutboundKind::GroupInvite);
        assert_eq!(ev.group_id, 777);
        ev.request.expect("request ref").invitation_seq
    }

    async fn wait_state(rx: &mut watch::Receiver<Option<LoginInfo>>, connected: bool) {
        tokio::time::timeout(
            Duration::from_secs(5),
            rx.wait_for(|s| s.is_some() == connected),
        )
        .await
        .expect("timed out waiting for connection state")
        .expect("state sender dropped");
    }

    #[tokio::test]
    async fn reconnects_after_close_frame_and_server_restart() {
        // Reserve a port, then free it so the client's first attempts fail
        // (Lagrange not up yet at bridge start).
        let addr = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();

        let (inbound_tx, mut inbound_rx) = mpsc::channel(16);
        let cfg = MilkyConfig {
            ws_endpoint: format!("ws://{addr}/event"),
            token: String::new(),
        };
        let policy = ReconnectPolicy {
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
            stable_after: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(2),
            alarm_after: Duration::from_millis(50),
            alarm_every: Duration::from_millis(50),
        };
        let client = Client::with_policy(&cfg, inbound_tx, policy).unwrap();
        let mut state = client.subscribe_state();
        client.start();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(state.borrow().is_none(), "nothing to connect to yet");

        let ws_conns = Arc::new(AtomicUsize::new(0));
        let logins = Arc::new(AtomicUsize::new(0));
        let server = Server::start(addr, ws_conns.clone(), logins.clone()).await;

        // Initial connect after retries; server then sends a Close frame.
        assert_eq!(next_seq(&mut inbound_rx).await, 1);
        // Reconnected on the same server after the Close frame.
        assert_eq!(next_seq(&mut inbound_rx).await, 2);
        wait_state(&mut state, true).await;

        // Full restart: server goes away, port unbound for a while, comes back.
        server.stop().await;
        wait_state(&mut state, false).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let server = Server::start(addr, ws_conns.clone(), logins.clone()).await;
        assert_eq!(next_seq(&mut inbound_rx).await, 3);
        wait_state(&mut state, true).await;
        assert_eq!(
            state.borrow().as_ref().map(|l| l.self_id),
            Some(10001),
            "login re-fetched over the HTTP API after the restart"
        );
        assert_eq!(
            logins.load(Ordering::SeqCst),
            3,
            "one login fetch per connect"
        );

        client.shutdown().await;
        wait_state(&mut state, false).await;
        server.stop().await;
        assert_eq!(ws_conns.load(Ordering::SeqCst), 3);
    }
}
