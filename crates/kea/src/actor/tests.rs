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
    SearchMode, UpsertDataRequest, VaultCreateKnowledgeRequest, VaultKnowledgeFilters, VaultListKnowledgeRequest,
    VaultSearchKnowledgeRequest,
};
use super::Actor;

#[test]
fn create_note_request_serializes_direct_vault_shape_and_input_file() {
    let payload = KnowledgeRequestPayload::CreateKnowledge {
        vault: None,
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

async fn request(actor: &mut Actor, payload: KnowledgeRequestPayload) -> Result<KnowledgeResponse, KnowledgeError> {
    let (reply_to, reply) = tokio::sync::oneshot::channel();
    actor
        .handle_message(KnowledgeRequest { payload, reply_to })
        .await;
    reply.await.expect("actor reply")
}

#[test]
fn search_mode_defaults_to_similarity_on_the_wire() {
    let payload: KnowledgeRequestPayload = serde_json::from_value(json!({"SearchKnowledge": {
        "request": {"query": "term", "filters": {}, "limit": 10}
    }}))
    .expect("default mode");
    assert!(matches!(
        payload,
        KnowledgeRequestPayload::SearchKnowledge {
            mode: SearchMode::Similarity,
            vault: None,
            ..
        }
    ));
    let input: super::super::messages::SearchKnowledgeInput = serde_json::from_value(json!({
        "request": {"query": "term", "filters": {}, "limit": 10}
    }))
    .expect("generated input defaults");
    assert!(input.vault.is_none());
    assert_eq!(input.mode, SearchMode::Similarity);
}

#[tokio::test]
async fn front_matter_matches_exact_values_without_using_query() {
    let (_directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let mut first = note("First", Some("text".to_owned()), None);
    first.frontmatter.insert("kind".to_owned(), json!("guide"));
    actor.handle_create_knowledge(first).await.expect("first");
    actor
        .handle_create_knowledge(note("Second", Some("text".to_owned()), None))
        .await
        .expect("second");
    let filters = VaultKnowledgeFilters {
        frontmatter: Some(Map::from_iter([("kind".to_owned(), json!("guide"))])),
        ..Default::default()
    };
    let search = VaultSearchKnowledgeRequest {
        query: "ignored".to_owned(),
        filters,
        limit: 10,
    };
    let result = actor
        .notes
        .search_mode(SearchMode::FrontMatter, &search)
        .await
        .expect("search");
    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].knowledge.path, "first.md");
}

#[tokio::test]
async fn search_modes_are_vault_scoped_and_full_text_is_lazy() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let root = directory.path().join("named");
    fs::create_dir_all(&root).await.expect("root");
    request(
        &mut actor,
        KnowledgeRequestPayload::RegisterVault {
            vault_id: "named".to_owned(),
            root,
        },
    )
    .await
    .expect("register");
    for (vault, title, content) in [
        (None, "Global", "common globalword"),
        (Some("named".to_owned()), "Named", "common namedword"),
    ] {
        let mut target = note(title, Some(content.to_owned()), None);
        target
            .frontmatter
            .insert("kind".to_owned(), json!("target"));
        request(
            &mut actor,
            KnowledgeRequestPayload::CreateKnowledge {
                vault: vault.clone(),
                request: target,
            },
        )
        .await
        .expect("create note");
        request(
            &mut actor,
            KnowledgeRequestPayload::CreateKnowledge {
                vault,
                request: note("Other", Some("unrelated words".to_owned()), None),
            },
        )
        .await
        .expect("create other note");
    }
    assert!(!actor.notes.full_text_is_open());
    assert!(
        !actor.vaults["named"]
            .runtime
            .as_ref()
            .expect("runtime")
            .notes
            .full_text_is_open()
    );
    for mode in [SearchMode::Similarity, SearchMode::FrontMatter, SearchMode::FullText] {
        for (vault, expected) in [(None, "global.md"), (Some("named".to_owned()), "named.md")] {
            let query = VaultSearchKnowledgeRequest {
                query: "common".to_owned(),
                filters: if mode == SearchMode::FrontMatter {
                    VaultKnowledgeFilters {
                        frontmatter: Some(Map::from_iter([("kind".to_owned(), json!("target"))])),
                        ..Default::default()
                    }
                } else {
                    VaultKnowledgeFilters::default()
                },
                limit: 100,
            };
            let KnowledgeResponse::KnowledgeSearched(result) = request(
                &mut actor,
                KnowledgeRequestPayload::SearchKnowledge {
                    vault,
                    mode,
                    request: query.clone(),
                },
            )
            .await
            .expect("search") else {
                panic!("search response")
            };
            assert_eq!(result.results.len(), 1);
            assert_eq!(result.results[0].knowledge.path, expected);
            if mode == SearchMode::Similarity && expected == "global.md" {
                let KnowledgeResponse::KnowledgeSearched(legacy) = request(
                    &mut actor,
                    KnowledgeRequestPayload::SearchKnowledge {
                        request: query,
                        vault: None,
                        mode: SearchMode::Similarity,
                    },
                )
                .await
                .expect("legacy search") else {
                    panic!("search response")
                };
                assert_eq!(result, legacy);
            }
        }
    }
    assert!(actor.notes.full_text_is_open());
    assert!(
        actor.vaults["named"]
            .runtime
            .as_ref()
            .expect("runtime")
            .notes
            .full_text_is_open()
    );
    let query = VaultSearchKnowledgeRequest {
        query: "common".to_owned(),
        filters: VaultKnowledgeFilters::default(),
        limit: 10,
    };
    assert!(matches!(
        request(
            &mut actor,
            KnowledgeRequestPayload::SearchKnowledge {
                vault: Some("missing".to_owned()),
                mode: SearchMode::Similarity,
                request: query
            }
        )
        .await,
        Err(KnowledgeError::VaultNotRegistered { .. })
    ));
}

#[tokio::test]
async fn full_text_tracks_create_and_delete() {
    let (_directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let query = VaultSearchKnowledgeRequest {
        query: "uniqueword".to_owned(),
        filters: VaultKnowledgeFilters::default(),
        limit: 10,
    };
    actor
        .notes
        .search_mode(SearchMode::FullText, &query)
        .await
        .expect("open index");
    actor
        .handle_create_knowledge(note("Fresh", Some("uniqueword".to_owned()), None))
        .await
        .expect("create");
    assert_eq!(
        actor
            .notes
            .search_mode(SearchMode::FullText, &query)
            .await
            .expect("search")
            .results
            .len(),
        1
    );
    actor
        .handle_delete_knowledge("fresh.md".to_owned())
        .await
        .expect("delete");
    assert!(
        actor
            .notes
            .search_mode(SearchMode::FullText, &query)
            .await
            .expect("search")
            .results
            .is_empty()
    );
}

#[tokio::test]
async fn named_vault_lifecycle_is_lazy_isolated_and_retryable() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let first = directory.path().join("first");
    let second = directory.path().join("second");
    let missing = directory.path().join("missing");
    fs::create_dir_all(&first).await.expect("first root");
    fs::create_dir_all(&second).await.expect("second root");
    for (id, root) in [("a", first.clone()), ("b", second.clone()), ("bad", missing.clone())] {
        request(
            &mut actor,
            KnowledgeRequestPayload::RegisterVault {
                vault_id: id.to_owned(),
                root,
            },
        )
        .await
        .expect("register");
    }
    assert!(actor.vaults["a"].runtime.is_none());
    assert!(matches!(
        request(
            &mut actor,
            KnowledgeRequestPayload::GetVaultStats {
                vault: Some("absent".to_owned())
            }
        )
        .await,
        Err(KnowledgeError::VaultNotRegistered { .. })
    ));
    assert!(matches!(
        request(
            &mut actor,
            KnowledgeRequestPayload::GetVaultStats {
                vault: Some("bad".to_owned())
            }
        )
        .await,
        Err(KnowledgeError::VaultUnavailable { .. })
    ));
    assert!(actor.vaults["bad"].runtime.is_none());
    request(
        &mut actor,
        KnowledgeRequestPayload::CreateKnowledge {
            vault: Some("a".to_owned()),
            request: note("Same", Some("first".to_owned()), None),
        },
    )
    .await
    .expect("create first");
    let first_monitor = actor.vaults["a"]
        .runtime
        .as_ref()
        .expect("opened")
        .notes
        .monitor_abort_handle()
        .expect("monitor");
    let opened = &actor.vaults["a"].runtime.as_ref().expect("opened").notes as *const _;
    request(
        &mut actor,
        KnowledgeRequestPayload::GetVaultStats {
            vault: Some("a".to_owned()),
        },
    )
    .await
    .expect("reuse");
    assert_eq!(
        opened,
        &actor.vaults["a"].runtime.as_ref().expect("opened").notes as *const _
    );
    request(
        &mut actor,
        KnowledgeRequestPayload::CreateKnowledge {
            vault: Some("b".to_owned()),
            request: note("Same", Some("second".to_owned()), None),
        },
    )
    .await
    .expect("create second");
    request(
        &mut actor,
        KnowledgeRequestPayload::CreateKnowledge {
            vault: None,
            request: note("Same", Some("global".to_owned()), None),
        },
    )
    .await
    .expect("create global");
    for (id, expected) in [("a", "first"), ("b", "second")] {
        let KnowledgeResponse::KnowledgeFound { knowledge } = request(
            &mut actor,
            KnowledgeRequestPayload::GetKnowledge {
                vault: Some(id.to_owned()),
                request: "same.md".to_owned(),
            },
        )
        .await
        .expect("read named") else {
            panic!("expected note")
        };
        assert_eq!(knowledge.content, expected);
    }
    let KnowledgeResponse::KnowledgeFound { knowledge } = request(
        &mut actor,
        KnowledgeRequestPayload::GetKnowledge {
            vault: None,
            request: "same.md".to_owned(),
        },
    )
    .await
    .expect("read global") else {
        panic!("expected global note")
    };
    assert_eq!(knowledge.content, "global");
    request(
        &mut actor,
        KnowledgeRequestPayload::RegisterVault {
            vault_id: "a".to_owned(),
            root: first,
        },
    )
    .await
    .expect("register same root");
    assert_eq!(
        opened,
        &actor.vaults["a"]
            .runtime
            .as_ref()
            .expect("still open")
            .notes as *const _
    );
    request(
        &mut actor,
        KnowledgeRequestPayload::RegisterVault {
            vault_id: "a".to_owned(),
            root: second,
        },
    )
    .await
    .expect("replace");
    tokio::task::yield_now().await;
    assert!(first_monitor.is_finished());
    assert!(actor.vaults["a"].runtime.is_none());
    let KnowledgeResponse::KnowledgeFound { knowledge } = request(
        &mut actor,
        KnowledgeRequestPayload::GetKnowledge {
            vault: Some("a".to_owned()),
            request: "same.md".to_owned(),
        },
    )
    .await
    .expect("reopened") else {
        panic!("expected note")
    };
    assert_eq!(knowledge.content, "second");
    let second_monitor = actor.vaults["a"]
        .runtime
        .as_ref()
        .expect("reopened")
        .notes
        .monitor_abort_handle()
        .expect("monitor");
    request(
        &mut actor,
        KnowledgeRequestPayload::UnregisterVault {
            vault_id: "a".to_owned(),
        },
    )
    .await
    .expect("unregister");
    tokio::task::yield_now().await;
    assert!(second_monitor.is_finished());
    assert!(matches!(
        request(
            &mut actor,
            KnowledgeRequestPayload::GetVaultStats {
                vault: Some("a".to_owned())
            }
        )
        .await,
        Err(KnowledgeError::VaultNotRegistered { .. })
    ));
    fs::create_dir_all(missing)
        .await
        .expect("create missing root");
    request(
        &mut actor,
        KnowledgeRequestPayload::GetVaultStats {
            vault: Some("bad".to_owned()),
        },
    )
    .await
    .expect("retry open");
}

#[tokio::test]
async fn named_create_uses_existing_collision_and_path_rules() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let root = directory.path().join("vault");
    fs::create_dir_all(&root).await.expect("root");
    request(
        &mut actor,
        KnowledgeRequestPayload::RegisterVault {
            vault_id: "a".to_owned(),
            root,
        },
    )
    .await
    .expect("register");
    let mut note_request = note("Same", Some("first".to_owned()), None);
    note_request.backend_id = Some("nested/note.md".to_owned());
    let KnowledgeResponse::KnowledgeCreated { knowledge: first } = request(
        &mut actor,
        KnowledgeRequestPayload::CreateKnowledge {
            vault: Some("a".to_owned()),
            request: note_request.clone(),
        },
    )
    .await
    .expect("first") else {
        panic!("created")
    };
    let KnowledgeResponse::KnowledgeCreated { knowledge: second } = request(
        &mut actor,
        KnowledgeRequestPayload::CreateKnowledge {
            vault: Some("a".to_owned()),
            request: note_request.clone(),
        },
    )
    .await
    .expect("second") else {
        panic!("created")
    };
    assert_eq!(first.backend_id, "nested/note.md");
    assert_eq!(second.backend_id, "nested/note-1.md");
    note_request.backend_id = Some("../escape.md".to_owned());
    assert!(
        request(
            &mut actor,
            KnowledgeRequestPayload::CreateKnowledge {
                vault: Some("a".to_owned()),
                request: note_request
            }
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn named_vault_stats_count_beyond_list_limit() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let root = directory.path().join("many");
    fs::create_dir_all(&root).await.expect("root");
    for index in 0..105 {
        fs::write(root.join(format!("{index}.md")), "content")
            .await
            .expect("write");
        fs::write(
            directory
                .path()
                .join("knowledge")
                .join(format!("{index}.md")),
            "content",
        )
        .await
        .expect("write global");
    }
    request(
        &mut actor,
        KnowledgeRequestPayload::RegisterVault {
            vault_id: "many".to_owned(),
            root,
        },
    )
    .await
    .expect("register");
    for vault in [None, Some("many".to_owned())] {
        let KnowledgeResponse::VaultStatsFound { stats } =
            request(&mut actor, KnowledgeRequestPayload::GetVaultStats { vault })
                .await
                .expect("stats")
        else {
            panic!("stats response")
        };
        assert_eq!(stats.document_count, 105);
    }
}

#[tokio::test]
async fn concurrent_named_requests_are_serialized_and_unregister_removes_the_runtime() {
    let (directory, mut actor) = actor(KnowledgeConfig::default()).await;
    let root = directory.path().join("concurrent");
    fs::create_dir_all(&root).await.expect("root");
    let (sender, receiver) = mpsc::channel(16);
    actor.receiver = receiver;
    let task = tokio::spawn(super::run_actor(actor));

    let (reply_to, reply) = tokio::sync::oneshot::channel();
    sender
        .send(KnowledgeRequest {
            payload: KnowledgeRequestPayload::RegisterVault {
                vault_id: "a".to_owned(),
                root,
            },
            reply_to,
        })
        .await
        .expect("register request");
    assert!(matches!(
        reply.await.expect("reply").expect("registered"),
        KnowledgeResponse::VaultRegistered
    ));

    let mut replies = Vec::new();
    for _ in 0..8 {
        let (reply_to, reply) = tokio::sync::oneshot::channel();
        sender
            .send(KnowledgeRequest {
                payload: KnowledgeRequestPayload::GetVaultStats {
                    vault: Some("a".to_owned()),
                },
                reply_to,
            })
            .await
            .expect("stats request");
        replies.push(reply);
    }
    for reply in replies {
        let KnowledgeResponse::VaultStatsFound { stats } = reply.await.expect("reply").expect("stats") else {
            panic!("expected stats");
        };
        assert_eq!(stats.document_count, 0);
    }
    let (reply_to, reply) = tokio::sync::oneshot::channel();
    sender
        .send(KnowledgeRequest {
            payload: KnowledgeRequestPayload::UnregisterVault {
                vault_id: "a".to_owned(),
            },
            reply_to,
        })
        .await
        .expect("unregister request");
    assert!(matches!(
        reply.await.expect("reply").expect("unregistered"),
        KnowledgeResponse::VaultUnregistered
    ));
    let (reply_to, reply) = tokio::sync::oneshot::channel();
    sender
        .send(KnowledgeRequest {
            payload: KnowledgeRequestPayload::GetVaultStats {
                vault: Some("a".to_owned()),
            },
            reply_to,
        })
        .await
        .expect("stats request");
    assert!(matches!(
        reply.await.expect("reply"),
        Err(KnowledgeError::VaultNotRegistered { .. })
    ));
    drop(sender);
    task.abort();
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
