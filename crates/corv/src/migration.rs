use aves::utils::database::Migration;

pub(crate) const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "discuss_init",
    sql: include_str!("../migrations/0001_discuss_init.sql"),
}];
