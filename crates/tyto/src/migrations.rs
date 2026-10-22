use aves::utils::database::Migration;

/// Migration ledger for Tyto's messaging database.
pub(super) const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "messaging_store",
    sql: include_str!("../migrations/0001_messaging_store.sql"),
}];
