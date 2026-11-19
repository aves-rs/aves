use crate::utils::database::Migration;

pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "coordination_state",
        sql: include_str!("./coordination_migrations/0001_coordination_state.sql"),
    },
    Migration {
        version: 2,
        name: "coordination_retired",
        sql: include_str!("./coordination_migrations/0002_coordination_retired.sql"),
    },
    Migration {
        version: 3,
        name: "coordination_domains",
        sql: include_str!("./coordination_migrations/0003_coordination_domains.sql"),
    },
];
