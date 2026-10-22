mod connection;
mod filter;
mod helpers;
mod plugin;
mod send;
mod subscribe;

pub(super) use plugin::EmailPlugin;

#[cfg(test)]
mod test;
