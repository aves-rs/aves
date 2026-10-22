use iroh::SecretKey;
use keyring::Entry;

use super::types::AvesAuthError;

const MACHINE_IDENTITY_ENTRY: &str = "auth-machine-identity";
const MACHINE_IDENTITY_LENGTH: usize = 32;

/// Aves-owned persistence for the runtime's Iroh machine identity.
pub(crate) struct MachineIdentityKeyring {
    #[cfg(test)]
    namespace: String,
    secret_key: SecretKey,
    #[cfg(test)]
    mock_store: Option<std::sync::Arc<keyring_core::mock::Store>>,
}

impl MachineIdentityKeyring {
    /// Open the machine-identity entry in the supplied keyring namespace.
    pub(crate) fn new(namespace: &str) -> Result<Self, AvesAuthError> {
        Self::from_store(namespace, None)
    }

    pub(crate) fn secret_key(&self) -> &SecretKey {
        &self.secret_key
    }

    #[cfg(not(test))]
    pub(crate) fn from_store(namespace: &str, _mock_store: Option<()>) -> Result<Self, AvesAuthError> {
        if namespace.is_empty() {
            return Err(AvesAuthError::EmptyKeyringNamespace);
        }

        let entry = Entry::new(namespace, MACHINE_IDENTITY_ENTRY)?;
        let secret_key = Self::load_or_create_entry(&entry)?;
        Ok(Self { secret_key })
    }

    #[cfg(test)]
    fn entry(&self) -> Result<Entry, AvesAuthError> {
        use keyring_core::api::CredentialStoreApi;

        if let Some(store) = &self.mock_store {
            return Ok(Entry {
                inner: store.build(&self.namespace, MACHINE_IDENTITY_ENTRY, None)?,
            });
        }

        Ok(Entry::new(&self.namespace, MACHINE_IDENTITY_ENTRY)?)
    }

    fn validated_secret(secret: Vec<u8>) -> Result<SecretKey, AvesAuthError> {
        if secret.len() != MACHINE_IDENTITY_LENGTH {
            return Err(AvesAuthError::InvalidMachineIdentitySecretLength {
                expected: MACHINE_IDENTITY_LENGTH,
                actual: secret.len(),
            });
        }
        let mut bytes = [0; MACHINE_IDENTITY_LENGTH];
        bytes.copy_from_slice(&secret);
        Ok(SecretKey::from_bytes(&bytes))
    }

    fn load_or_create_entry(entry: &Entry) -> Result<SecretKey, AvesAuthError> {
        match entry.get_secret() {
            Ok(secret) => Self::validated_secret(secret),
            Err(keyring::Error::NoEntry) => {
                let secret_key = SecretKey::generate();
                entry.set_secret(&secret_key.to_bytes())?;
                Ok(secret_key)
            }
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_mock(namespace: &str) -> Result<Self, AvesAuthError> {
        Self::from_store(
            namespace,
            Some(keyring_core::mock::Store::new().expect("mock keyring store")),
        )
    }

    #[cfg(test)]
    pub(super) fn from_store(
        namespace: &str,
        mock_store: Option<std::sync::Arc<keyring_core::mock::Store>>,
    ) -> Result<Self, AvesAuthError> {
        use keyring_core::api::CredentialStoreApi;

        if namespace.is_empty() {
            return Err(AvesAuthError::EmptyKeyringNamespace);
        }

        let entry = if let Some(store) = &mock_store {
            Entry {
                inner: store.build(namespace, MACHINE_IDENTITY_ENTRY, None)?,
            }
        } else {
            Entry::new(namespace, MACHINE_IDENTITY_ENTRY)?
        };
        let secret_key = Self::load_or_create_entry(&entry)?;
        Ok(Self {
            namespace: namespace.to_owned(),
            secret_key,
            mock_store,
        })
    }

    #[cfg(test)]
    fn delete(&self) -> Result<bool, AvesAuthError> {
        match self.entry()?.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring::Error::NoEntry) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_namespace() {
        assert!(matches!(
            MachineIdentityKeyring::new_mock(""),
            Err(AvesAuthError::EmptyKeyringNamespace)
        ));
    }

    #[test]
    fn machine_identity_roundtrips_and_deletes() {
        let store = keyring_core::mock::Store::new().expect("mock keyring store");
        let first = MachineIdentityKeyring::from_store("aves-test", Some(store.clone())).expect("identity is created");
        let second = MachineIdentityKeyring::from_store("aves-test", Some(store)).expect("identity is loaded");
        assert_eq!(first.secret_key().to_bytes(), second.secret_key().to_bytes());

        assert!(first.delete().expect("identity deletes"));
        assert!(!first.delete().expect("missing identity remains absent"));
    }

    #[test]
    fn exposes_a_stable_iroh_public_key() {
        let keyring = MachineIdentityKeyring::new_mock("aves-test").expect("keyring opens");
        let public_key = keyring.secret_key().public();
        let parsed: iroh::PublicKey = public_key
            .to_string()
            .parse()
            .expect("issuer is an Iroh public key");
        assert_eq!(parsed, public_key);
    }
}
