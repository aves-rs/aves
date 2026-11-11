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
];
