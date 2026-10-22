use serde::{Deserialize, Serialize};
use specta::Type;

/// Specta marker exported as TypeScript `bigint` by Zeus's binding normalizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
pub struct SpectaBigInt;
