use crate::utils::database::Migration;

pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "flow_definitions_and_execution_snapshots",
        sql: include_str!("./flow_migrations/0001_flow_definitions_and_execution_snapshots.sql"),
    },
    Migration {
        version: 2,
        name: "flow_execution_documents",
        sql: include_str!("./flow_migrations/0002_flow_execution_documents.sql"),
    },
    Migration {
        version: 3,
        name: "flow_workspace_checkpoints",
        sql: include_str!("./flow_migrations/0003_flow_workspace_checkpoints.sql"),
    },
    Migration {
        version: 4,
        name: "flow_automatic_enabled",
        sql: include_str!("./flow_migrations/0004_flow_automatic_enabled.sql"),
    },
    Migration {
        version: 5,
        name: "flow_workspace_mapping",
        sql: include_str!("./flow_migrations/0005_flow_workspace_mapping.sql"),
    },
    Migration {
        version: 6,
        name: "flow_imported_workspace_root",
        sql: include_str!("./flow_migrations/0006_flow_imported_workspace_root.sql"),
    },
    Migration {
        version: 7,
        name: "flow_node_execution_dataflow",
        sql: include_str!("./flow_migrations/0007_flow_node_execution_dataflow.sql"),
    },
    Migration {
        version: 9,
        name: "flow_mobility",
        sql: include_str!("./flow_migrations/0009_flow_mobility.sql"),
    },
];
