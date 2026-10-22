ALTER TABLE flow_definition_revisions ADD COLUMN automatic_enabled INTEGER NOT NULL DEFAULT 1 CHECK (automatic_enabled IN (0, 1));
