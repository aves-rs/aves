//! Unpublished utility domains for Aves applications.

mod subscription;

pub use subscription::{
    SubscriptionBroadcast, SubscriptionDomain, SubscriptionError, rpc_router,
    subscription_operation_descriptors_with_root_error,
};
