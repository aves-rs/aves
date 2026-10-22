CREATE TABLE IF NOT EXISTS knowledge (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    content_type TEXT NOT NULL CHECK (content_type IN ('markdown', 'plaintext')),
    kind TEXT NOT NULL CHECK (kind IN ('note', 'capture', 'generated', 'reference')),
    source_type TEXT NOT NULL CHECK (source_type IN ('manual', 'url', 'file', 'agent', 'generated')),
    source_uri TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    searchable_metadata_text TEXT,
    content_hash TEXT NOT NULL,
    index_status TEXT NOT NULL DEFAULT 'pending' CHECK (index_status IN ('pending', 'ready', 'failed')),
    index_error TEXT,
    index_updated_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_knowledge_updated_at ON knowledge (updated_at DESC, id);
CREATE INDEX IF NOT EXISTS idx_knowledge_created_at ON knowledge (created_at DESC, id);
CREATE INDEX IF NOT EXISTS idx_knowledge_index_status ON knowledge (index_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_kind ON knowledge (kind, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_content_type ON knowledge (content_type, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_source_type ON knowledge (source_type, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_source_uri ON knowledge (source_uri) WHERE source_uri IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_knowledge_content_hash ON knowledge (content_hash);

CREATE TABLE IF NOT EXISTS tags (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    path TEXT NOT NULL,
    normalized_path TEXT NOT NULL UNIQUE,
    parent_id TEXT REFERENCES tags(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tags_parent_id ON tags (parent_id);
CREATE INDEX IF NOT EXISTS idx_tags_normalized_name ON tags (normalized_name);

CREATE TABLE IF NOT EXISTS knowledge_tags (
    knowledge_id TEXT NOT NULL REFERENCES knowledge(id) ON DELETE CASCADE,
    tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (knowledge_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_tags_tag_id ON knowledge_tags (tag_id, knowledge_id);

CREATE TABLE IF NOT EXISTS knowledge_chunks (
    id TEXT PRIMARY KEY NOT NULL,
    knowledge_id TEXT NOT NULL REFERENCES knowledge(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    content TEXT NOT NULL,
    token_count INTEGER NOT NULL CHECK (token_count >= 0),
    content_hash TEXT NOT NULL,
    start_byte INTEGER CHECK (start_byte IS NULL OR start_byte >= 0),
    end_byte INTEGER CHECK (end_byte IS NULL OR end_byte >= 0),
    split_metadata_json TEXT NOT NULL DEFAULT '{}',
    chunking_strategy TEXT NOT NULL,
    chunking_strategy_version TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (knowledge_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_knowledge_id ON knowledge_chunks (knowledge_id, chunk_index);
CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_content_hash ON knowledge_chunks (content_hash);

CREATE TABLE IF NOT EXISTS embedding_models (
    id TEXT PRIMARY KEY NOT NULL,
    provider TEXT NOT NULL,
    model_id TEXT NOT NULL,
    profile_name TEXT NOT NULL,
    tokenizer_id TEXT NOT NULL,
    tokenizer_artifact TEXT,
    dimensions INTEGER NOT NULL CHECK (dimensions > 0),
    max_tokens INTEGER NOT NULL CHECK (max_tokens > 0),
    target_tokens INTEGER NOT NULL CHECK (target_tokens > 0),
    overlap_tokens INTEGER NOT NULL CHECK (overlap_tokens >= 0),
    query_prefix TEXT NOT NULL DEFAULT '',
    passage_prefix TEXT NOT NULL DEFAULT '',
    chunking_strategy TEXT NOT NULL,
    chunking_strategy_version TEXT NOT NULL,
    vector_storage_mode TEXT NOT NULL DEFAULT 'blob' CHECK (vector_storage_mode IN ('blob', 'json', 'external')),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (provider, model_id, profile_name, tokenizer_id, dimensions, chunking_strategy_version)
);

CREATE INDEX IF NOT EXISTS idx_embedding_models_profile_name ON embedding_models (profile_name);
CREATE INDEX IF NOT EXISTS idx_embedding_models_model_id ON embedding_models (provider, model_id);

CREATE TABLE IF NOT EXISTS chunk_embeddings (
    chunk_id TEXT NOT NULL REFERENCES knowledge_chunks(id) ON DELETE CASCADE,
    embedding_model_id TEXT NOT NULL REFERENCES embedding_models(id) ON DELETE CASCADE,
    vector_blob BLOB,
    vector_json TEXT,
    vector_dimensions INTEGER NOT NULL CHECK (vector_dimensions > 0),
    vector_norm REAL,
    content_hash TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chunk_id, embedding_model_id),
    CHECK (vector_blob IS NOT NULL OR vector_json IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_model_id ON chunk_embeddings (embedding_model_id);
CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_content_hash ON chunk_embeddings (content_hash);

CREATE TABLE IF NOT EXISTS embedding_jobs (
    id TEXT PRIMARY KEY NOT NULL,
    knowledge_id TEXT NOT NULL REFERENCES knowledge(id) ON DELETE CASCADE,
    chunk_id TEXT REFERENCES knowledge_chunks(id) ON DELETE CASCADE,
    embedding_model_id TEXT REFERENCES embedding_models(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'ready', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    last_error TEXT,
    next_attempt_at TEXT,
    locked_at TEXT,
    locked_by TEXT,
    completed_at TEXT,
    job_metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_embedding_jobs_status_next_attempt ON embedding_jobs (status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_embedding_jobs_knowledge_id ON embedding_jobs (knowledge_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_embedding_jobs_chunk_id ON embedding_jobs (chunk_id) WHERE chunk_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_embedding_jobs_model_id ON embedding_jobs (embedding_model_id);

CREATE TABLE IF NOT EXISTS knowledge_search_entries (
    search_key TEXT PRIMARY KEY NOT NULL,
    knowledge_id TEXT NOT NULL REFERENCES knowledge(id) ON DELETE CASCADE,
    chunk_id TEXT REFERENCES knowledge_chunks(id) ON DELETE CASCADE,
    entry_kind TEXT NOT NULL CHECK (entry_kind IN ('knowledge', 'chunk')),
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    tags_text TEXT NOT NULL DEFAULT '',
    metadata_text TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK ((entry_kind = 'knowledge' AND chunk_id IS NULL) OR (entry_kind = 'chunk' AND chunk_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_knowledge_search_entries_knowledge_id ON knowledge_search_entries (knowledge_id);
CREATE INDEX IF NOT EXISTS idx_knowledge_search_entries_chunk_id ON knowledge_search_entries (chunk_id) WHERE chunk_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_knowledge_search_entries_entry_kind ON knowledge_search_entries (entry_kind, knowledge_id);

CREATE TABLE IF NOT EXISTS knowledge_search_terms (
    term TEXT NOT NULL,
    search_key TEXT NOT NULL REFERENCES knowledge_search_entries(search_key) ON DELETE CASCADE,
    knowledge_id TEXT NOT NULL REFERENCES knowledge(id) ON DELETE CASCADE,
    chunk_id TEXT REFERENCES knowledge_chunks(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL CHECK (field_name IN ('title', 'content', 'tags', 'metadata')),
    term_count INTEGER NOT NULL DEFAULT 1 CHECK (term_count > 0),
    first_position INTEGER CHECK (first_position IS NULL OR first_position >= 0),
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (term, search_key, field_name)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_search_terms_search_key ON knowledge_search_terms (search_key);
CREATE INDEX IF NOT EXISTS idx_knowledge_search_terms_knowledge_id ON knowledge_search_terms (knowledge_id, term);
CREATE INDEX IF NOT EXISTS idx_knowledge_search_terms_chunk_id ON knowledge_search_terms (chunk_id, term) WHERE chunk_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_knowledge_search_terms_field_name ON knowledge_search_terms (field_name, term);
