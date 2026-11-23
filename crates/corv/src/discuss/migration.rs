use aves::utils::database::Migration;

pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "discuss_init",
        sql: include_str!("../../migrations/0001_discuss_init.sql"),
    },
    Migration {
        version: 2,
        name: "session_launch_context",
        sql: include_str!("../../migrations/0002_session_launch_context.sql"),
    },
    Migration {
        version: 3,
        name: "session_launch_config",
        sql: include_str!("../../migrations/0003_session_launch_config.sql"),
    },
    Migration {
        version: 4,
        name: "session_setup_kind",
        sql: include_str!("../../migrations/0004_session_setup_kind.sql"),
    },
    Migration {
        version: 5,
        name: "accounting_ledgers",
        sql: include_str!("../../migrations/0005_accounting_ledgers.sql"),
    },
    Migration {
        version: 6,
        name: "custom_assets",
        sql: include_str!("../../migrations/0006_custom_assets.sql"),
    },
    Migration {
        version: 7,
        name: "discuss_identity",
        sql: include_str!("../../migrations/0007_discuss_identity.sql"),
    },
    Migration {
        version: 8,
        name: "custom_mcp",
        sql: include_str!("../../migrations/0008_custom_mcp.sql"),
    },
    Migration {
        version: 9,
        name: "session_launch_limitations",
        sql: include_str!("../../migrations/0009_session_launch_limitations.sql"),
    },
    Migration {
        version: 10,
        name: "session_modes",
        sql: include_str!("../../migrations/0010_session_modes.sql"),
    },
];
