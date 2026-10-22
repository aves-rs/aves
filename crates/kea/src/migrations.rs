use aves::utils::database::Migration;

pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 2,
        name: "knowledge",
        sql: include_str!("../migrations/0002_knowledge.sql"),
    },
    Migration {
        version: 3,
        name: "knowledge_data",
        sql: include_str!("../migrations/0003_knowledge_data.sql"),
    },
];
