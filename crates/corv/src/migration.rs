use aves::utils::database::Migration;

pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "discuss_init",
        sql: include_str!("../migrations/0001_discuss_init.sql"),
    },
    Migration {
        version: 2,
        name: "session_launch_context",
        sql: include_str!("../migrations/0002_session_launch_context.sql"),
    },
    Migration {
        version: 3,
        name: "session_launch_config",
        sql: include_str!("../migrations/0003_session_launch_config.sql"),
    },
    Migration {
        version: 4,
        name: "session_setup_kind",
        sql: include_str!("../migrations/0004_session_setup_kind.sql"),
    },
];
