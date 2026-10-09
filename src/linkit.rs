//! The Linkit notification connection. [`ensure`] provisions a Bot owned by
//! the user and stores its token, so the channel never needs manual
//! configuration; the user only sends messages or verifies the channel.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use reqwest::Method;
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::db::{Database, DatabaseError, LinkitStatus, SecretLinkitSettings};

/// The production Linkit deployment; tests point the same functions at a mock.
pub const API_URL: &str = "https://linkit.ntnl.io";

/// Every provisioned Bot is created with this name.
const BOT_NAME: &str = "Firmament";

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

const TEST_BODY: &str = "Firmament notification test / 通知测试\n\nThis verifies delivery to your Linkit conversation, not device push or read status.\n这条消息用于验证 Linkit 会话投递，不代表设备推送或已读。";

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

/// Serializes [`ensure`] calls per user so two concurrent workspace loads never
/// provision two Bots for one account.
static CONNECTION_LOCKS: LazyLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Error)]
pub enum LinkitError {
    #[error("state storage failed")]
    Database(#[from] DatabaseError),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    Unavailable(String),
    #[error("Linkit request failed: {0}")]
    Request(#[from] reqwest::Error),
}

#[derive(Deserialize)]
struct Identity {
    id: String,
    profile: Option<Profile>,
}

#[derive(Deserialize)]
struct Profile {
    username: String,
}

#[derive(Deserialize)]
struct OwnedBot {
    id: String,
    owner_user_id: String,
}

#[derive(Deserialize)]
struct BotGrant {
    id: String,
    token: String,
}

#[derive(Deserialize)]
struct DirectConversation {
    id: String,
    kind: String,
    counterpart_user_id: Option<String>,
}

#[derive(Deserialize)]
struct DeliveryReceipt {
    conversation_id: String,
    sender_id: String,
    sender_kind: String,
}

async fn lock_connection(owner_id: &str) -> OwnedMutexGuard<()> {
    let lock = {
        let mut locks = CONNECTION_LOCKS.lock().await;
        locks.entry(owner_id.to_owned()).or_default().clone()
    };
    lock.lock_owned().await
}

fn request(
    method: Method,
    api_url: &str,
    bearer: &str,
    segments: &[&str],
) -> Result<reqwest::RequestBuilder, LinkitError> {
    let mut url = reqwest::Url::parse(api_url)
        .map_err(|_| LinkitError::Unavailable("Linkit API URL is invalid".to_owned()))?;
    url.path_segments_mut()
        .map_err(|_| LinkitError::Unavailable("Linkit API URL is invalid".to_owned()))?
        .pop_if_empty()
        .extend(segments);
    Ok(CLIENT
        .request(method, url)
        .bearer_auth(bearer)
        .timeout(REQUEST_TIMEOUT))
}

fn checked(response: reqwest::Response, operation: &str) -> Result<reqwest::Response, LinkitError> {
    if !response.status().is_success() {
        return Err(LinkitError::Unavailable(format!(
            "Linkit {operation} failed (HTTP {})",
            response.status().as_u16()
        )));
    }
    Ok(response)
}

async fn token_matches(
    api_url: &str,
    connection: &SecretLinkitSettings,
) -> Result<bool, LinkitError> {
    let response = request(Method::GET, api_url, &connection.bot_token, &["api", "me"])?
        .send()
        .await?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Ok(false);
    }
    let identity = checked(response, "Bot authentication")?
        .json::<Identity>()
        .await?;
    Ok(identity.id == connection.bot_id)
}

fn save_grant(
    database: &Database,
    owner_id: &str,
    username: &str,
    grant: BotGrant,
) -> Result<SecretLinkitSettings, LinkitError> {
    if grant.id.is_empty() || !grant.token.starts_with("sk-") {
        return Err(LinkitError::Unavailable(
            "Linkit returned an invalid Bot credential".to_owned(),
        ));
    }
    let connection = SecretLinkitSettings {
        bot_id: grant.id,
        bot_token: grant.token,
        username: username.to_owned(),
    };
    // RECOVERY: a newly issued Bot token is only returned once; persist it
    // before any later remote check can fail so a retry can reuse it.
    database.save_linkit_bot(
        owner_id,
        &connection.bot_id,
        &connection.bot_token,
        username,
    )?;
    Ok(connection)
}

async fn create_bot(
    database: &Database,
    api_url: &str,
    owner_id: &str,
    bearer: &str,
    username: &str,
) -> Result<SecretLinkitSettings, LinkitError> {
    let grant = checked(
        request(Method::POST, api_url, bearer, &["api", "bots"])?
            .json(&json!({"name": BOT_NAME}))
            .send()
            .await?,
        "create Bot",
    )?
    .json::<BotGrant>()
    .await?;
    save_grant(database, owner_id, username, grant)
}

async fn repair_bot(
    database: &Database,
    api_url: &str,
    owner_id: &str,
    bearer: &str,
    stored: Option<SecretLinkitSettings>,
    username: &str,
) -> Result<SecretLinkitSettings, LinkitError> {
    let bots = checked(
        request(Method::GET, api_url, bearer, &["api", "bots"])?
            .send()
            .await?,
        "list owned Bots",
    )?
    .json::<Vec<OwnedBot>>()
    .await?;
    let Some(connection) = stored.filter(|connection| {
        bots.iter()
            .any(|bot| bot.id == connection.bot_id && bot.owner_user_id == owner_id)
    }) else {
        return create_bot(database, api_url, owner_id, bearer, username).await;
    };
    if token_matches(api_url, &connection).await? {
        return Ok(connection);
    }
    let grant = checked(
        request(
            Method::PATCH,
            api_url,
            bearer,
            &["api", "bots", &connection.bot_id],
        )?
        .json(&json!({"rotate_token": true}))
        .send()
        .await?,
        "rotate Bot token",
    )?
    .json::<BotGrant>()
    .await?;
    if grant.id != connection.bot_id {
        return Err(LinkitError::Unavailable(
            "Linkit rotated a different Bot".to_owned(),
        ));
    }
    save_grant(database, owner_id, username, grant)
}

/// The stored connection view; the Bot token is never part of it.
pub fn status(database: &Database, owner_id: &str) -> Result<LinkitStatus, LinkitError> {
    Ok(database.linkit_status(owner_id)?)
}

/// Idempotent: brings the user's Linkit connection to a complete, working
/// state — the account username, an owned Bot and a valid token. The workspace
/// calls this on every load so the channel is always ready and self-repairing.
pub async fn ensure(
    database: &Database,
    api_url: &str,
    owner_id: &str,
    bearer: &str,
) -> Result<LinkitStatus, LinkitError> {
    let _guard = lock_connection(owner_id).await;
    let stored = database.linkit_connection(owner_id)?;
    let owner = checked(
        request(Method::GET, api_url, bearer, &["api", "me"])?
            .send()
            .await?,
        "account lookup",
    )?
    .json::<Identity>()
    .await?;
    if owner.id != owner_id {
        return Err(LinkitError::Forbidden(
            "Linkit returned a different account".to_owned(),
        ));
    }
    let username = owner
        .profile
        .map(|profile| profile.username)
        .filter(|username| !username.trim().is_empty())
        .ok_or_else(|| {
            LinkitError::Conflict(
                "Set your Linkit username before notifications can be connected".to_owned(),
            )
        })?;
    let connection = repair_bot(database, api_url, owner_id, bearer, stored, &username).await?;
    database.save_linkit_bot(
        owner_id,
        &connection.bot_id,
        &connection.bot_token,
        &username,
    )?;
    if !token_matches(api_url, &connection).await? {
        return Err(LinkitError::Unavailable(
            "Linkit Bot credential changed during setup; repair the connection again".to_owned(),
        ));
    }
    status(database, owner_id)
}

async fn deliver(
    database: &Database,
    api_url: &str,
    owner_id: &str,
    body: &str,
) -> Result<(), LinkitError> {
    let Some(connection) = database.linkit_connection(owner_id)? else {
        return Err(LinkitError::Conflict(
            "Linkit notifications are not connected yet".to_owned(),
        ));
    };
    if !token_matches(api_url, &connection).await? {
        return Err(LinkitError::Unavailable(
            "Linkit Bot token is invalid; repair the connection and try again".to_owned(),
        ));
    }
    let conversation = checked(
        request(
            Method::POST,
            api_url,
            &connection.bot_token,
            &["api", "conversations", "direct", &connection.username],
        )?
        .send()
        .await?,
        "open direct conversation",
    )?
    .json::<DirectConversation>()
    .await?;
    // INVARIANT: a stored username can be renamed or reassigned in Linkit;
    // confirm the stable recipient UUID before disclosing any message.
    if conversation.id.is_empty()
        || conversation.kind != "direct"
        || conversation.counterpart_user_id.as_deref() != Some(owner_id)
    {
        return Err(LinkitError::Forbidden(
            "Linkit notification recipient changed; repair the connection and try again".to_owned(),
        ));
    }
    let receipt = checked(
        request(
            Method::POST,
            api_url,
            &connection.bot_token,
            &["api", "conversations", &conversation.id, "messages"],
        )?
        .json(&json!({"body": body, "attachment_ids": [], "urgent": false}))
        .send()
        .await?,
        "send message",
    )?
    .json::<DeliveryReceipt>()
    .await?;
    if receipt.conversation_id != conversation.id
        || receipt.sender_id != connection.bot_id
        || receipt.sender_kind != "bot"
    {
        return Err(LinkitError::Unavailable(
            "Linkit returned an invalid delivery receipt".to_owned(),
        ));
    }
    Ok(())
}

async fn deliver_and_record(
    database: &Database,
    api_url: &str,
    owner_id: &str,
    body: &str,
) -> Result<(), LinkitError> {
    let result = deliver(database, api_url, owner_id, body).await;
    let error = result.as_ref().err().map(ToString::to_string);
    database.record_linkit_delivery(owner_id, error.as_deref())?;
    result
}

/// Sends the manual test message and records the delivery.
pub async fn send_test(
    database: &Database,
    api_url: &str,
    owner_id: &str,
) -> Result<(), LinkitError> {
    deliver_and_record(database, api_url, owner_id, TEST_BODY).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        Json, Router,
        extract::State,
        http::{Request, StatusCode, header},
        response::{IntoResponse, Response},
    };
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use super::*;

    #[derive(Clone)]
    struct MockBot {
        id: String,
        token: String,
    }

    struct MockLinkit {
        bot: Option<MockBot>,
        username: Option<String>,
        calls: Vec<String>,
        messages: Vec<Value>,
        creates: usize,
        rotations: usize,
        fault: Option<(usize, StatusCode)>,
    }

    async fn mock_linkit(
        State(remote): State<Arc<Mutex<MockLinkit>>>,
        request: Request<axum::body::Body>,
    ) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        let bearer = request
            .headers()
            .get(header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body: Value = if bytes.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        let mut remote = remote.lock().await;
        remote.calls.push(format!("{method} {path}"));
        if let Some((n, status)) = remote.fault
            && remote.calls.len() == n
        {
            return (status, Json(json!({}))).into_response();
        }
        let human = bearer == "Bearer owner-session";
        let bot = remote
            .bot
            .as_ref()
            .filter(|bot| bearer == format!("Bearer {}", bot.token))
            .cloned();
        if !human && bot.is_none() {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if path == "/api/me" {
            return if human {
                Json(json!({
                    "id": "owner-1",
                    "profile": remote.username.as_ref().map(|username| json!({"username": username})),
                }))
                .into_response()
            } else {
                Json(json!({"id": bot.unwrap().id, "profile": null})).into_response()
            };
        }
        if path == "/api/bots" {
            assert!(human);
            if method == Method::GET {
                return Json(json!(
                    remote
                        .bot
                        .iter()
                        .map(|bot| json!({"id": bot.id, "owner_user_id": "owner-1"}))
                        .collect::<Vec<_>>()
                ))
                .into_response();
            }
            assert_eq!(body, json!({"name": "Firmament"}));
            remote.creates += 1;
            let created = MockBot {
                id: format!("created-{}", remote.creates),
                token: format!("sk-created-{}", remote.creates),
            };
            let result = json!({"id": created.id, "token": created.token});
            remote.bot = Some(created);
            return Json(result).into_response();
        }
        if path.starts_with("/api/bots/") {
            assert!(human);
            assert_eq!(method, Method::PATCH);
            assert_eq!(body, json!({"rotate_token": true}));
            remote.rotations += 1;
            let rotation = remote.rotations;
            let bot = remote.bot.as_mut().unwrap();
            assert_eq!(path, format!("/api/bots/{}", bot.id));
            bot.token = format!("sk-rotated-{rotation}");
            return Json(json!({"id": bot.id, "token": bot.token})).into_response();
        }
        assert!(!human);
        let bot = bot.unwrap();
        if path.starts_with("/api/conversations/direct/") {
            return Json(json!({
                "id": "conversation-1",
                "kind": "direct",
                "counterpart_user_id": "owner-1",
            }))
            .into_response();
        }
        if path == "/api/conversations/conversation-1/messages" {
            remote.messages.push(body);
            return Json(json!({
                "id": format!("message-{}", remote.messages.len()),
                "conversation_id": "conversation-1",
                "sender_id": bot.id,
                "sender_kind": "bot",
            }))
            .into_response();
        }
        panic!("unexpected Linkit route {method} {path}");
    }

    struct Fixture {
        _state: TempDir,
        database: Database,
        remote: Arc<Mutex<MockLinkit>>,
        api_url: String,
        server: tokio::task::JoinHandle<()>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    async fn fixture() -> Fixture {
        let state = TempDir::new().unwrap();
        let database = Database::open(state.path()).unwrap();
        let remote = Arc::new(Mutex::new(MockLinkit {
            bot: None,
            username: Some("alice".into()),
            calls: vec![],
            messages: vec![],
            creates: 0,
            rotations: 0,
            fault: None,
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .fallback(mock_linkit)
            .with_state(remote.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Fixture {
            _state: state,
            database,
            remote,
            api_url,
            server,
        }
    }

    async fn ensure_connection(f: &Fixture) -> Result<LinkitStatus, LinkitError> {
        ensure(&f.database, &f.api_url, "owner-1", "owner-session").await
    }

    fn stored(f: &Fixture) -> SecretLinkitSettings {
        f.database
            .linkit_connection("owner-1")
            .unwrap()
            .expect("connection is stored")
    }

    #[tokio::test]
    async fn ensure_provisions_a_bot_and_reuses_it_on_later_loads() {
        let f = fixture().await;
        let status = ensure_connection(&f).await.unwrap();
        assert!(status.configured);
        assert_eq!(status.recipient_username.as_deref(), Some("alice"));
        assert_eq!(status.bot_id.as_deref(), Some("created-1"));
        assert_eq!(f.remote.lock().await.creates, 1);
        let serialized = serde_json::to_string(&status).unwrap();
        assert!(!serialized.contains("sk-"));

        let again = ensure_connection(&f).await.unwrap();
        assert!(again.configured);
        let remote = f.remote.lock().await;
        assert_eq!(remote.creates, 1);
        assert_eq!(remote.rotations, 0);
    }

    #[tokio::test]
    async fn ensure_refreshes_the_username_and_rotates_a_stale_token() {
        let f = fixture().await;
        ensure_connection(&f).await.unwrap();
        {
            let mut remote = f.remote.lock().await;
            remote.username = Some("renamed".into());
            remote.bot.as_mut().unwrap().token = "sk-elsewhere".into();
        }
        let status = ensure_connection(&f).await.unwrap();
        assert_eq!(status.recipient_username.as_deref(), Some("renamed"));
        assert_eq!(f.remote.lock().await.rotations, 1);
        let stored = stored(&f);
        assert_eq!(stored.bot_token, "sk-rotated-1");
        assert_eq!(stored.username, "renamed");
    }

    #[tokio::test]
    async fn ensure_recreates_a_deleted_bot() {
        let f = fixture().await;
        ensure_connection(&f).await.unwrap();
        f.remote.lock().await.bot = None;
        let status = ensure_connection(&f).await.unwrap();
        assert!(status.configured);
        assert_eq!(status.bot_id.as_deref(), Some("created-2"));
        assert_eq!(f.remote.lock().await.creates, 2);
    }

    #[tokio::test]
    async fn parallel_ensure_calls_create_only_one_bot() {
        let f = fixture().await;
        let (first, second) = tokio::join!(ensure_connection(&f), ensure_connection(&f));
        assert!(first.unwrap().configured);
        assert!(second.unwrap().configured);
        let remote = f.remote.lock().await;
        assert_eq!(remote.creates, 1);
        assert_eq!(remote.rotations, 0);
    }

    #[tokio::test]
    async fn new_bot_token_is_saved_before_final_validation_failure_and_reused_on_retry() {
        let f = fixture().await;
        f.remote.lock().await.fault = Some((4, StatusCode::SERVICE_UNAVAILABLE));
        assert!(ensure_connection(&f).await.is_err());
        assert_eq!(stored(&f).bot_token, "sk-created-1");
        let status = ensure_connection(&f).await.unwrap();
        assert!(status.configured);
        assert_eq!(f.remote.lock().await.creates, 1);
    }

    #[tokio::test]
    async fn send_test_requires_the_connection_and_records_the_delivery() {
        let f = fixture().await;
        assert!(matches!(
            send_test(&f.database, &f.api_url, "owner-1").await,
            Err(LinkitError::Conflict(_))
        ));
        ensure_connection(&f).await.unwrap();
        send_test(&f.database, &f.api_url, "owner-1").await.unwrap();
        let remote = f.remote.lock().await;
        assert_eq!(remote.messages.len(), 1);
        assert!(
            remote.messages[0]["body"]
                .as_str()
                .unwrap()
                .contains("通知测试")
        );
        drop(remote);
        let status = f.database.linkit_status("owner-1").unwrap();
        assert!(status.last_success_at.is_some());
        assert!(status.last_error.is_none());
    }
}
