use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use specta::datatype::reference::Reference;
use specta::{DataType, Generics, Type, TypeCollection};
use uuid::{Builder, Error as UuidError, Uuid, Variant, Version};

/// A Specta-compatible wrapper around `uuid::Uuid` that maps to a string in TypeScript.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct SpectaUuid(#[schemars(with = "String")] pub Uuid);

impl Type for SpectaUuid {
    fn inline(type_map: &mut TypeCollection, generics: Generics<'_>) -> DataType {
        String::inline(type_map, generics)
    }

    fn reference(type_map: &mut TypeCollection, generics: &[DataType]) -> Reference {
        String::reference(type_map, generics)
    }
}

impl Deref for SpectaUuid {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Uuid> for SpectaUuid {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<SpectaUuid> for Uuid {
    fn from(wrapper: SpectaUuid) -> Self {
        wrapper.0
    }
}

impl fmt::Display for SpectaUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for SpectaUuid {
    type Err = UuidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(Self)
    }
}

/// Deterministically derive a UUID from string parts.
///
/// Hashes the length-prefixed parts with SHA-256 and folds the digest into a
/// 16-byte UUID with the version/variant nibbles set. Stable across runs and
/// processes for the same input, so it is suitable for content-addressed IDs.
pub fn deterministic_uuid_from_parts(parts: &[&str]) -> Uuid {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    let digest: [u8; 32] = hasher.finalize().into();
    let bytes: [u8; 16] = digest[..16]
        .try_into()
        .expect("digest has at least 16 bytes");
    Builder::from_bytes(bytes)
        .with_version(Version::Sha1)
        .with_variant(Variant::RFC4122)
        .into_uuid()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_uuid_is_stable_for_same_parts() {
        let a = deterministic_uuid_from_parts(&["application", "record", "first"]);
        let b = deterministic_uuid_from_parts(&["application", "record", "first"]);
        assert_eq!(a, b);
    }

    #[test]
    fn deterministic_uuid_differs_for_different_parts() {
        let a = deterministic_uuid_from_parts(&["application", "record", "first"]);
        let b = deterministic_uuid_from_parts(&["application", "record", "second"]);
        assert_ne!(a, b);
    }

    #[test]
    fn deterministic_uuid_is_length_prefixed_not_concatenated() {
        // Length-prefixing must prevent ["ab","c"] from colliding with ["a","bc"].
        let a = deterministic_uuid_from_parts(&["ab", "c"]);
        let b = deterministic_uuid_from_parts(&["a", "bc"]);
        assert_ne!(a, b);
    }

    #[test]
    fn deterministic_uuid_matches_independent_golden_vectors() {
        let cases = [
            (&[][..], "e3b0c442-98fc-5c14-9afb-f4c8996fb924"),
            (&[""][..], "af5570f5-a181-5b7a-b78c-af4bc70a660f"),
            (&["é"][..], "1d9b2f68-e31e-52b6-b30c-67d7729ca6a5"),
            (&["alpha", "beta"][..], "5d11bfa6-2398-519b-90c0-222e68c98272"),
            (&["beta", "alpha"][..], "fb0fe886-7c82-5687-b785-8073d552dfc7"),
            (&["a:b", "c"][..], "88701777-2657-5d82-966c-fb38c4bc7c2d"),
            (&["a", "b:c"][..], "fdd00f5e-cd1d-5148-abec-e13763c08e60"),
        ];

        for (parts, expected) in cases {
            assert_eq!(deterministic_uuid_from_parts(parts).to_string(), expected);
        }
    }

    #[test]
    fn deterministic_uuid_sets_sha1_version_and_rfc4122_variant() {
        let uuid = deterministic_uuid_from_parts(&["version and variant"]);

        assert_eq!(uuid.get_version(), Some(Version::Sha1));
        assert_eq!(uuid.get_variant(), Variant::RFC4122);
    }

    #[test]
    fn specta_uuid_json_roundtrips_as_a_string() {
        let uuid = SpectaUuid(Uuid::from_u128(0x123456789abcdef0123456789abcdef0));

        let json = serde_json::to_string(&uuid).unwrap();
        assert_eq!(json, format!("\"{uuid}\""));
        assert_eq!(serde_json::from_str::<SpectaUuid>(&json).unwrap(), uuid);
    }

    #[test]
    fn specta_uuid_rejects_invalid_null_and_number_json() {
        for json in ["\"not-a-uuid\"", "null", "42"] {
            assert!(serde_json::from_str::<SpectaUuid>(json).is_err());
        }
    }

    #[test]
    fn specta_uuid_schema_and_specta_type_are_strings() {
        let schema = schemars::schema_for!(SpectaUuid);
        assert_eq!(schema.as_value()["type"], "string");

        let projected = SpectaUuid::inline(&mut TypeCollection::default(), Generics::NONE);
        let string_type = String::inline(&mut TypeCollection::default(), Generics::NONE);
        assert_eq!(projected, string_type);
    }
}
