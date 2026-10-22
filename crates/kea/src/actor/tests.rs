use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use serde_json::{Map, json};
use tempfile::{TempDir, tempdir};
use tokio::fs;
use tokio::sync::mpsc;

use aves::auth::Jwt;
use aves::bus::BusRpcContext;

use super::super::documents::open_documents;
use super::super::messages::{KnowledgeRequest, KnowledgeRequestPayload, KnowledgeResponse};
use super::super::types::{
    CreateCanvasDashboardRequest, GetCanvasDashboardRequest, KnowledgeConfig, KnowledgeContentType, KnowledgeError,
    UpsertDataRequest, VaultCreateKnowledgeRequest, VaultKnowledgeFilters, VaultListKnowledgeRequest,
};
use super::Actor;

#[test]
fn create_note_request_serializes_direct_vault_shape_and_input_file() {
    let payload = KnowledgeRequestPayload::CreateKnowledge {
        request: VaultCreateKnowledgeRequest {
            backend_id: Some("research/note.md".to_owned()),
            title: "Note".to_owned(),
            content: None,
            input_file: Some(PathBuf::from("notes/source.md")),
            content_type: KnowledgeContentType::Markdown,
            frontmatter: Map::new(),
            tags: vec!["research".to_owned()],
        },
    };
    let encoded = serde_json::to_value(&payload).expect("serialize note request");
    assert_eq!(encoded["CreateKnowledge"]["request"]["backend_id"], "research/note.md");
    assert_eq!(encoded["CreateKnowledge"]["request"]["input_file"], "notes/source.md");
    assert!(
        encoded["CreateKnowledge"]["request"]
            .get("Database")
            .is_none()
    );
}

#[test]
fn data_requests_preserve_exact_case_and_json_object() {
    let payload = KnowledgeRequestPayload::UpsertData {
        request: UpsertDataRequest {
            key: "Mixed/Case".to_owned(),
            value: serde_json::from_value(json!({"nested": {"count": 3}})).expect("JSON object"),
        },
    };
    let encoded = serde_json::to_value(&payload).expect("serialize data request");
    assert_eq!(encoded["UpsertData"]["request"]["key"], "Mixed/Case");
    assert_eq!(encoded["UpsertData"]["request"]["value"]["nested"]["count"], 3);
    let roundtrip: KnowledgeRequestPayload = serde_json::from_value(encoded).expect("deserialize data request");
    assert!(matches!(roundtrip, KnowledgeRequestPayload::UpsertData { request } if request.key == "Mixed/Case"));
    let not_an_object = json!({"UpsertData": {"request": {"key": "x", "value": [1,2]}}});
    assert!(serde_json::from_value::<KnowledgeRequestPayload>(not_an_object).is_err());
}

async fn actor(config: KnowledgeConfig) -> (TempDir, Actor) {
    let directory = tempdir().expect("configuration directory");
    let (requests, _receiver) = mpsc::channel(8);
    let (broadcasts, _broadcast_rx) = mpsc::channel(8);
    let (_tx, messages) = mpsc::channel(8);
    let config_dir = directory.path().to_path_buf();
    let root = Arc::new(RwLock::new(
        config
            .root
            .clone()
            .unwrap_or_else(|| config_dir.join("knowledge")),
    ));
    let actor = Actor::new(
        messages,
        BusRpcContext::authenticated(requests, Jwt::from_opaque("test-jwt".to_owned())),
        config_dir,
        config,
        root,
        broadcasts,
    )
    .await;
    (directory, actor)
}

fn note(title: &str, content: Option<String>, input_file: Option<PathBuf>) -> VaultCreateKnowledgeRequest {
    VaultCreateKnowledgeRequest {
        backend_id: None,
        title: title.to_owned(),
        content,
        input_file,
        content_type: KnowledgeContentType::Markdown,
        frontmatter: Map::new(),
        tags: Vec::new(),
    }
}

#[tokio::test]
async fn default_directory_is_created_and_note_crud_uses_files_only() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    assert!(directory.path().join("knowledge").is_dir());
    let KnowledgeResponse::KnowledgeCreated { knowledge } = actor
        .handle_create_knowledge(note("My Note", Some("hello".to_owned()), None))
        .await
        .expect("create note")
    else {
        panic!("expected created note")
    };
    assert!(
        directory
            .path()
            .join("knowledge")
            .join(&knowledge.backend_id)
            .is_file()
    );
    let KnowledgeResponse::KnowledgeFound { knowledge: found } = actor
        .handle_get_knowledge(knowledge.backend_id.clone())
        .await
        .expect("read note")
    else {
        panic!("expected note")
    };
    assert_eq!(found.content, "hello");
    let KnowledgeResponse::KnowledgeDeleted { result } = actor
        .handle_delete_knowledge(knowledge.backend_id.clone())
        .await
        .expect("delete note")
    else {
        panic!("expected deletion")
    };
    assert!(result.deleted);
}

#[tokio::test]
async fn relative_input_file_uses_only_the_fixed_config_discuss_directory() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let source_directory = directory.path().join("discuss").join("reports");
    fs::create_dir_all(&source_directory)
        .await
        .expect("source directory");
    fs::write(source_directory.join("result.md"), "relative source")
        .await
        .expect("source file");

    let response = actor
        .handle_create_knowledge(note("Imported", None, Some(PathBuf::from("reports/result.md"))))
        .await
        .expect("relative import");

    let KnowledgeResponse::KnowledgeCreated { knowledge } = response else {
        panic!("expected created note");
    };
    assert_eq!(knowledge.content, "relative source");
}

#[tokio::test]
async fn missing_relative_input_file_reports_the_resolved_path_and_absolute_path_guidance() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let missing = directory.path().join("discuss").join("missing.md");

    let error = actor
        .handle_create_knowledge(note(
            "Missing",
            Some("do not fall back".to_owned()),
            Some(PathBuf::from("missing.md")),
        ))
        .await
        .expect_err("missing relative input must fail");

    let KnowledgeError::Io { path, message } = error else {
        panic!("expected input file I/O error");
    };
    assert_eq!(path, missing.display().to_string());
    assert!(message.contains(&missing.display().to_string()));
    assert!(message.contains("Agents should use absolute paths"));
    assert!(message.contains("relative paths are resolved only under config_dir/discuss"));
}

#[test]
fn create_knowledge_schema_advises_agents_to_use_absolute_input_paths() {
    let schema = serde_json::to_value(schemars::schema_for!(VaultCreateKnowledgeRequest)).expect("schema");
    let description = schema["properties"]["input_file"]["description"]
        .as_str()
        .expect("input_file description");

    assert!(description.contains("Agents SHOULD use an absolute path"));
    assert!(description.contains("config_dir/discuss"));
}

#[tokio::test]
async fn configured_directory_must_exist_without_creating_it() {
    let directory = tempdir().expect("tempdir");
    let config = KnowledgeConfig {
        root: Some(directory.path().join("missing")),
        ..KnowledgeConfig::default()
    };
    let error = open_documents(directory.path(), &config)
        .await
        .err()
        .expect("missing configured root must fail");
    assert!(matches!(error, KnowledgeError::InvalidBackendConfig { .. }));
    assert!(!config.root.expect("root").exists());
}

#[tokio::test]
async fn input_file_overrides_inline_content_and_directory_import_preserves_nested_paths() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let source = directory.path().join("source.md");
    fs::write(&source, "from file").await.expect("source");
    let KnowledgeResponse::KnowledgeCreated { knowledge } = actor
        .handle_create_knowledge(note("Imported", Some("inline".to_owned()), Some(source)))
        .await
        .expect("import file")
    else {
        panic!("expected created note")
    };
    assert_eq!(knowledge.content, "from file");

    let source_dir = directory.path().join("sources");
    fs::create_dir_all(source_dir.join("nested"))
        .await
        .expect("nested source");
    fs::write(source_dir.join("nested/child.md"), "nested")
        .await
        .expect("child source");
    let KnowledgeResponse::KnowledgeCreated { knowledge } = actor
        .handle_create_knowledge(note("Folder", None, Some(source_dir)))
        .await
        .expect("import directory")
    else {
        panic!("expected created note")
    };
    assert_eq!(knowledge.path, "nested/child.md");
    let listed = actor
        .notes
        .list(VaultListKnowledgeRequest {
            query: None,
            filters: VaultKnowledgeFilters::default(),
            limit: 10,
        })
        .await
        .expect("list notes");
    assert_eq!(listed.len(), 2);
}

#[tokio::test]
async fn missing_note_reports_exact_path() {
    let (_directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let error = actor
        .handle_get_knowledge("missing.md".to_owned())
        .await
        .expect_err("missing note");
    assert!(matches!(error, KnowledgeError::KnowledgeNotFound { identifier, .. } if identifier == "missing.md"));
}

#[tokio::test]
async fn canvas_dashboard_still_roundtrips() {
    let (_directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let KnowledgeResponse::CanvasDashboardCreated { dashboard } = actor
        .handle_create_canvas_dashboard(CreateCanvasDashboardRequest {
            title: "Dashboard".to_owned(),
            html: "<p>hello</p>".to_owned(),
        })
        .await
        .expect("create dashboard")
    else {
        panic!("expected created dashboard")
    };
    let KnowledgeResponse::CanvasDashboardFound { dashboard: found } = actor
        .handle_get_canvas_dashboard(GetCanvasDashboardRequest { id: dashboard.id })
        .await
        .expect("read dashboard")
    else {
        panic!("expected found dashboard")
    };
    assert_eq!(found.0.expect("dashboard").html, "<p>hello</p>");
}

#[tokio::test]
async fn stop_runtime_replies_after_database_maintenance() {
    let (_directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let (reply_to, reply) = tokio::sync::oneshot::channel();

    actor
        .handle_message(KnowledgeRequest {
            payload: KnowledgeRequestPayload::StopRuntime,
            reply_to,
        })
        .await;

    assert!(matches!(
        reply.await.expect("runtime stop reply"),
        Ok(KnowledgeResponse::RuntimeStopped)
    ));
}
