use serde::{Deserialize, Serialize};
use specta::Type;

use super::types::DiscussMemberOverrides;
/// Theme-backed colors available for a Phanes Agent mark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AgentThemeColor {
    /// Light shade from the primary gradient.
    PrimaryLight,
    /// Primary application color.
    Primary,
    /// Dark shade from the primary gradient.
    PrimaryDark,
    /// Violet identity color.
    Violet,
    /// Plum identity color.
    Grape,
    /// Orange identity color.
    Orange,
    /// Warning semantic color.
    Warning,
    /// Error semantic color.
    Error,
    /// Primary text color.
    Text,
    /// Dimmed text color.
    Dimmed,
}

/// Public metadata for a built-in skill.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinSkillEntry {
    pub skill_id: String,
    pub name: String,
    pub description: String,
    pub version: String,
    /// Canonical `SKILL.md` contents from the application-supplied definition.
    pub body: String,
}

/// Source of an installed asset claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstalledClaimBasis {
    /// A standalone custom installation.
    Standalone,
    /// A team installation.
    Team,
}

/// Recorded asset installation, not a filesystem existence check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstalledAssetClaim {
    pub provider_name: String,
    pub asset_id: String,
    pub basis: InstalledClaimBasis,
    pub team_leader_id: Option<String>,
    pub installed_version: Option<String>,
    pub scope_key: Option<String>,
    pub provider_installed: bool,
    pub definition_present: bool,
}

/// An installed team whose missing definition prevents skill enumeration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IncompleteTeamClaim {
    pub provider_name: String,
    pub team_leader_id: String,
}

/// Metadata and provenance for a cached custom asset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CustomAssetMetadata {
    pub id: String,
    pub name: String,
    pub job_title: Option<String>,
    pub description: String,
    pub version: String,
    pub source_kind: String,
    pub source_url: Option<String>,
    pub last_pull_at_ms: Option<i64>,
    pub last_pull_ok: Option<bool>,
    pub pull_error: Option<String>,
}

/// Kind of installed asset addressed by workspace instructions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstalledAssetKind {
    /// An agent file.
    Agent,
    /// A skill's SKILL.md file.
    Skill,
}

/// A file in a custom skill bundle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CustomSkillBundleFile {
    pub relative_path: String,
    pub contents: String,
}

/// A cached custom agent and its authored body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CustomAgentDetail {
    pub metadata: CustomAssetMetadata,
    pub body: String,
}

/// A cached custom skill and its complete bundle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CustomSkillDetail {
    pub metadata: CustomAssetMetadata,
    pub files: Vec<CustomSkillBundleFile>,
}

/// One canonical Phanes Team member in display order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TeamCatalogMember {
    /// Stable Phanes Agent identifier used for member overrides.
    pub agent_id: String,
    /// Human-readable Agent name.
    pub name: String,
    /// Human-readable Agent job title.
    pub job_title: String,
    /// Agent body (system prompt) from its built-in or custom definition.
    pub body: String,
}

/// Origin of the currently available Team definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TeamDefinitionKind {
    /// An application-supplied Team definition.
    Builtin,
    /// A user-defined Team definition.
    Custom,
    /// An installation whose definition is no longer available.
    Unavailable,
}

/// One Phanes Team and its persisted installation state.
///
/// Teams are identified by their stable leader id. The member and skill names
/// are optional because a persisted installation may outlive its registry
/// definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Type, ::schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TeamCatalogEntry {
    /// Stable Phanes Team leader identifier.
    pub leader_id: String,
    /// Current or persisted Phanes Team version.
    pub version: String,
    /// Canonical Team name when its definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Canonical leader role when its definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_title: Option<String>,
    /// Canonical Team description when its definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether this Team has a persisted installation.
    pub installed: bool,
    /// Installed provider identifier, when installed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    /// Team-member-specific provider config selections, when installed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_overrides: Option<DiscussMemberOverrides>,
    /// User-selected theme color for the Team mark.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub theme_color: Option<AgentThemeColor>,
    /// Whether the current Phanes registry contains this Team definition.
    pub definition_available: bool,
    /// Origin of the Team definition, or unavailable for an orphaned installation.
    pub definition_kind: TeamDefinitionKind,
    /// Canonical Team members, including stable IDs, when the definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<Vec<TeamCatalogMember>>,
    /// Canonical skill display names when the Team definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_names: Option<Vec<String>>,
    /// Ordered stable skill identifiers when the Team definition is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_ids: Option<Vec<String>>,
}
