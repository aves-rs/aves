DROP TABLE IF EXISTS knowledge_search_terms;
DROP TABLE IF EXISTS knowledge_search_entries;
DROP TABLE IF EXISTS embedding_jobs;
DROP TABLE IF EXISTS chunk_embeddings;
DROP TABLE IF EXISTS knowledge_chunks;
DROP TABLE IF EXISTS knowledge_tags;
DROP TABLE IF EXISTS embedding_models;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS knowledge;

CREATE TABLE knowledge_data (
    key TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    value_json TEXT NOT NULL CHECK (json_valid(value_json) AND json_type(value_json) = 'object'),
    updated_at TEXT NOT NULL
);

CREATE INDEX knowledge_data_updated_at ON knowledge_data (updated_at DESC, key);
