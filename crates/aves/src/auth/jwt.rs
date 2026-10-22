use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use pkcs8::{
    AlgorithmIdentifierRef, ObjectIdentifier, PrivateKeyInfoRef, SecretDocument,
    der::{Encode, asn1::OctetStringRef},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::keystore::MachineIdentityKeyring;
use super::types::{AuthCapability, AvesAuthError, MachineIssuerId};

const ROOT_SUBJECT: &str = "root";
const ED25519_OID: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.3.101.112");

/// An opaque compact signed JWT/JWS carrier.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, specta::Type, schemars::JsonSchema)]
#[serde(transparent)]
pub struct Jwt(String);

/// Wraps an externally supplied compact token without interpreting it.
///
/// Transport adapters may carry the token, but claims remain private to
/// Aves authentication.
impl Jwt {
    pub fn from_opaque(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Jwt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// Claims understood by Aves Auth. This type never crosses the Auth boundary.
#[derive(Clone, Debug)]
pub(super) struct AuthClaims {
    iss: MachineIssuerId,
    aud: Uuid,
    sub: String,
    jti: Uuid,
    exp: DateTime<Utc>,
    cap: Vec<AuthCapability>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireClaims {
    iss: String,
    aud: Uuid,
    sub: String,
    jti: Uuid,
    exp: i64,
    cap: Vec<AuthCapability>,
}

impl From<AuthClaims> for WireClaims {
    fn from(claims: AuthClaims) -> Self {
        Self {
            iss: claims.iss.to_string(),
            aud: claims.aud,
            sub: claims.sub,
            jti: claims.jti,
            exp: claims.exp.timestamp(),
            cap: claims.cap,
        }
    }
}

impl TryFrom<WireClaims> for AuthClaims {
    type Error = AvesAuthError;

    fn try_from(claims: WireClaims) -> Result<Self, Self::Error> {
        let exp = DateTime::from_timestamp(claims.exp, 0).ok_or(AvesAuthError::MalformedJwt)?;
        if claims.sub.is_empty() {
            return Err(AvesAuthError::InvalidSubject);
        }
        Ok(Self {
            iss: claims
                .iss
                .parse()
                .map_err(|_| AvesAuthError::MalformedJwt)?,
            aud: claims.aud,
            sub: claims.sub,
            jti: claims.jti,
            exp,
            cap: claims.cap,
        })
    }
}

impl AuthClaims {
    pub(super) fn jti(&self) -> Uuid {
        self.jti
    }
    pub(super) fn expiration(&self) -> DateTime<Utc> {
        self.exp
    }
    pub(super) fn subject(&self) -> String {
        self.sub.clone()
    }
    pub(super) fn capabilities(&self) -> Vec<AuthCapability> {
        self.cap.clone()
    }
}

pub(super) fn issue_jwt(
    keyring: &MachineIdentityKeyring,
    audience: Uuid,
    subject: String,
    capabilities: Vec<AuthCapability>,
    expires_at: DateTime<Utc>,
) -> Result<Jwt, AvesAuthError> {
    if subject.is_empty() || subject == "root" {
        return Err(AvesAuthError::InvalidSubject);
    }
    issue_jwt_unchecked(keyring, audience, subject, capabilities, expires_at)
}

pub(super) fn issue_root_jwt(
    keyring: &MachineIdentityKeyring,
    audience: Uuid,
    expires_at: DateTime<Utc>,
) -> Result<Jwt, AvesAuthError> {
    issue_jwt_unchecked(
        keyring,
        audience,
        ROOT_SUBJECT.to_owned(),
        vec![AuthCapability::AllBus],
        expires_at,
    )
}

pub(super) fn verify_jwt(jwt: &Jwt, issuer: &MachineIssuerId, audience: Uuid) -> Result<AuthClaims, AvesAuthError> {
    verify_jwt_unchecked(jwt, issuer, audience)
}

pub fn issuer_from_jwt(jwt: &Jwt) -> Result<MachineIssuerId, AvesAuthError> {
    let payload = jwt
        .as_str()
        .split('.')
        .nth(1)
        .ok_or(AvesAuthError::MalformedJwt)?;
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| AvesAuthError::MalformedJwt)?;
    let claims: WireClaims = serde_json::from_slice(&payload).map_err(|_| AvesAuthError::MalformedJwt)?;
    claims.iss.parse().map_err(|_| AvesAuthError::MalformedJwt)
}

fn issue_jwt_unchecked(
    keyring: &MachineIdentityKeyring,
    audience: Uuid,
    subject: String,
    capabilities: Vec<AuthCapability>,
    expires_at: DateTime<Utc>,
) -> Result<Jwt, AvesAuthError> {
    let encoding_key = encoding_key(keyring.secret_key())?;

    let claims = AuthClaims {
        iss: keyring.secret_key().public().into(),
        aud: audience,
        sub: subject,
        jti: Uuid::new_v4(),
        exp: expires_at,
        cap: capabilities,
    };

    let token = encode(&Header::new(Algorithm::EdDSA), &WireClaims::from(claims), &encoding_key)
        .map_err(|error| AvesAuthError::Internal(error.to_string()))?;

    Ok(Jwt(token))
}

fn verify_jwt_unchecked(jwt: &Jwt, issuer: &MachineIssuerId, audience: Uuid) -> Result<AuthClaims, AvesAuthError> {
    let public_key_component = URL_SAFE_NO_PAD.encode(issuer.verification_key());

    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_issuer(&[issuer.to_string()]);
    validation.set_audience(&[audience.to_string()]);
    validation.set_required_spec_claims(&["iss", "aud", "sub", "exp"]);
    validation.leeway = 0;
    validation.validate_exp = true;

    let decoding_key = DecodingKey::from_ed_components(&public_key_component)
        .map_err(|error| AvesAuthError::Internal(error.to_string()))?;
    let decoded =
        decode::<WireClaims>(jwt.as_str(), &decoding_key, &validation).map_err(|error| match error.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AvesAuthError::ExpiredToken,
            jsonwebtoken::errors::ErrorKind::InvalidIssuer => AvesAuthError::UnknownIssuer,
            jsonwebtoken::errors::ErrorKind::InvalidAudience => AvesAuthError::WrongAudience,
            jsonwebtoken::errors::ErrorKind::InvalidSignature => AvesAuthError::SignatureInvalid,
            _ => AvesAuthError::MalformedJwt,
        })?;

    let claims = AuthClaims::try_from(decoded.claims)?;
    if claims.aud != audience {
        return Err(AvesAuthError::WrongAudience);
    }

    if &claims.iss != issuer {
        return Err(AvesAuthError::UnknownIssuer);
    }

    Ok(claims)
}

fn encoding_key(secret_key: &iroh::SecretKey) -> Result<EncodingKey, AvesAuthError> {
    let secret_bytes = secret_key.to_bytes();
    let curve_private_key =
        OctetStringRef::new(&secret_bytes).map_err(|error| AvesAuthError::Internal(error.to_string()))?;
    let curve_private_key_der = curve_private_key
        .to_der()
        .map_err(|error| AvesAuthError::Internal(error.to_string()))?;
    let private_key =
        OctetStringRef::new(&curve_private_key_der).map_err(|error| AvesAuthError::Internal(error.to_string()))?;
    let private_key_info = PrivateKeyInfoRef::new(
        AlgorithmIdentifierRef {
            oid: ED25519_OID,
            parameters: None,
        },
        private_key,
    );
    let private_der =
        SecretDocument::try_from(&private_key_info).map_err(|error| AvesAuthError::Internal(error.to_string()))?;
    Ok(EncodingKey::from_ed_der(private_der.as_bytes()))
}
