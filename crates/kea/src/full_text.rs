use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::OnceCell;
use turbovault_tools::{SearchEngine, SearchQuery, SearchResultInfo};
use turbovault_vault::VaultManager;

use super::types::KnowledgeResult;

/// One lazily opened full-text index for a document runtime.
pub(super) struct FullText {
    engine: OnceCell<Arc<SearchEngine>>,
    directory: PathBuf,
}

impl FullText {
    pub(super) fn new(config_dir: &Path, vault: &Path) -> Self {
        let hash = turbovault_core::compute_hash(&vault.to_string_lossy());
        Self {
            engine: OnceCell::new(),
            directory: config_dir.join("vault-search").join(&hash[..16]),
        }
    }

    pub(super) async fn changes(&self, changes: Vec<(String, bool)>) {
        if let Some(engine) = self.engine.get()
            && let Err(error) = engine.apply_changes(changes).await
        {
            tracing::error!(%error, "full-text index update failed");
        }
    }

    pub(super) async fn search(
        &self,
        manager: &Arc<VaultManager>,
        query: &str,
        limit: usize,
    ) -> KnowledgeResult<Vec<SearchResultInfo>> {
        manager.ensure_fresh().await;
        let engine = self
            .engine
            .get_or_try_init(|| async {
                SearchEngine::open(Arc::clone(manager), &self.directory)
                    .await
                    .map(Arc::new)
            })
            .await?;
        Ok(engine
            .advanced_search(SearchQuery::new(query).limit(limit))
            .await?)
    }

    #[cfg(test)]
    pub(super) fn is_open(&self) -> bool {
        self.engine.get().is_some()
    }
}
