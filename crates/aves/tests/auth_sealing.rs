#![allow(unused_crate_dependencies)]

#[test]
fn auth_public_surfaces_contain_no_auth_operations() {
    let operations = aves::messages::AUTH_REQUEST_PAYLOAD_OPERATIONS;
    assert!(operations.is_empty());

    let router = aves::auth_rpc_router();
    let paths = (&router)
        .into_iter()
        .map(|(path, _)| {
            path.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("/")
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(paths.is_empty());
}
