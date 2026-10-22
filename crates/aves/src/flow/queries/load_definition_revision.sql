SELECT r.name, r.graph_json, r.blackboard_defaults_json, r.editor_metadata_json, r.automatic_enabled, r.workspace_mapping_json FROM flow_definition_revisions r WHERE r.flow_id=?1 AND r.revision=?2
