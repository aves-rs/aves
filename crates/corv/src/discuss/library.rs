use std::fmt;
use std::path::Path;
use std::sync::Arc;

use semver::Version;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Application-supplied, immutable definitions used by Discuss.
#[derive(Clone, Copy, Debug)]
pub struct AssetLibrary {
    agents: &'static [AgentDefinition],
    skills: &'static [SkillDefinition],
    commands: &'static [CommandDefinition],
    teams: &'static [TeamDefinition],
}

impl AssetLibrary {
    /// Supply the host application's embedded catalog; Corv has no built-in content.
    pub fn new(
        agents: &'static [AgentDefinition],
        skills: &'static [SkillDefinition],
        commands: &'static [CommandDefinition],
        teams: &'static [TeamDefinition],
    ) -> Self {
        Self {
            agents,
            skills,
            commands,
            teams,
        }
    }

    /// Iterate over supplied agents in canonical order.
    pub fn agents_for(self) -> impl Iterator<Item = &'static AgentDefinition> {
        self.agents.iter()
    }

    /// Iterate over supplied skills in canonical order.
    pub fn skills_for(self) -> impl Iterator<Item = &'static SkillDefinition> {
        self.skills.iter()
    }

    /// Iterate over supplied commands in canonical order.
    pub fn commands_for(self) -> impl Iterator<Item = &'static CommandDefinition> {
        self.commands.iter()
    }

    /// Iterate over supplied teams in canonical order.
    pub fn teams_for(self) -> impl Iterator<Item = &'static TeamDefinition> {
        self.teams.iter()
    }
}

/// A compile-time embedded canonical agent definition.
#[derive(Debug, Clone)]
pub struct AgentAssetDefinition<Id> {
    /// Stable asset identity.
    pub id: Id,

    /// Registry version for this agent.
    pub version: Version,

    /// The role this agent occupies in canonical teams.
    pub role: AgentRole,

    /// Human-readable agent name
    pub name: &'static str,

    /// Human-readable job title
    pub job_title: &'static str,

    /// Human-readable asset description.
    pub description: &'static str,

    /// Compile-time embedded asset body.
    pub body: &'static str,
}

pub type AgentDefinition = AgentAssetDefinition<AgentId>;

/// An owned, user-defined agent.
#[derive(Debug, Clone)]
pub(crate) struct CustomAgentDefinition {
    pub(crate) id: AgentId,
    pub(crate) name: String,
    pub(crate) job_title: String,
    pub(crate) description: String,
    pub(crate) body: String,
    pub(crate) version: Version,
}

/// An owned file in a custom skill bundle.
#[derive(Debug, Clone)]
pub(crate) struct CustomSkillFile {
    pub(crate) relative_path: String,
    pub(crate) contents: String,
}

/// An owned, user-defined skill and its files.
#[derive(Debug, Clone)]
pub(crate) struct CustomSkillDefinition {
    pub(crate) id: SkillId,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) bundle: Vec<CustomSkillFile>,
    pub(crate) version: Version,
}

/// An owned, user-defined team identified by its leader.
#[derive(Debug, Clone)]
pub(crate) struct CustomTeamDefinition {
    pub(crate) leader: AgentId,
    pub(crate) followers: Vec<AgentId>,
    pub(crate) skills: Vec<SkillId>,
    pub(crate) version: Version,
}

/// A resolved built-in or custom agent.
#[derive(Debug, Clone)]
pub(crate) enum ResolvedAgent {
    /// An application-supplied definition.
    Builtin(&'static AgentDefinition),
    /// A runtime-owned definition.
    Custom(Arc<CustomAgentDefinition>),
}

impl ResolvedAgent {
    /// Borrows the fields needed to select this agent at session launch.
    pub(crate) fn selection_view(&self) -> AgentSelectionView<'_> {
        match self {
            Self::Builtin(agent) => AgentSelectionView::from_builtin(agent),
            Self::Custom(agent) => AgentSelectionView::from_custom(agent, None),
        }
    }
}

/// A resolved built-in or custom skill.
#[derive(Debug, Clone)]
pub(crate) enum ResolvedSkill {
    /// An application-supplied definition.
    Builtin(&'static SkillDefinition),
    /// A runtime-owned definition.
    Custom(Arc<CustomSkillDefinition>),
}

/// A resolved built-in or custom team.
#[derive(Debug, Clone)]
pub(crate) enum ResolvedTeam {
    /// An application-supplied definition.
    Builtin(&'static TeamDefinition),
    /// A runtime-owned definition.
    Custom(Arc<CustomTeamDefinition>),
}

/// Borrowed agent fields needed for provider selection.
#[derive(Debug, Clone)]
pub(crate) struct AgentSelectionView<'a> {
    pub(crate) id: &'a AgentId,
    pub(crate) description: &'a str,
    pub(crate) body: &'a str,
    /// The agent's place in the team being materialized; `None` when installed on its own.
    pub(crate) role: Option<AgentRole>,
}

impl<'a> AgentSelectionView<'a> {
    /// Borrows selection fields from a static built-in definition.
    pub(crate) fn from_builtin(agent: &'static AgentDefinition) -> Self {
        Self {
            id: &agent.id,
            description: agent.description,
            body: agent.body,
            role: Some(agent.role),
        }
    }

    /// Borrows selection fields from a custom definition, with its role in the team being materialized.
    pub(crate) fn from_custom(agent: &'a CustomAgentDefinition, role: Option<AgentRole>) -> Self {
        Self {
            id: &agent.id,
            description: &agent.description,
            body: &agent.body,
            role,
        }
    }
}

/// The initial backend-assigned version of a custom asset.
pub(crate) fn initial_custom_version() -> Version {
    Version::new(0, 1, 0)
}

/// Returns the next patch version, or `None` when the patch is exhausted.
pub(crate) fn bump_custom_version(version: &Version) -> Option<Version> {
    Some(Version::new(
        version.major,
        version.minor,
        version.patch.checked_add(1)?,
    ))
}

/// Validates a custom bundle destination path.
pub(crate) fn validate_custom_relative_path(path: &str) -> Result<(), Error> {
    validate_relative_path(path)
}

/// Validates custom bundle destinations and the required first `SKILL.md` file.
pub(crate) fn validate_custom_skill_bundle(bundle: &[CustomSkillFile]) -> Result<(), Error> {
    for file in bundle {
        validate_relative_path(&file.relative_path)?;
    }
    if bundle.first().map(|file| file.relative_path.as_str()) != Some("SKILL.md") {
        return Err(Error::InvalidAssetId {
            asset_id: bundle
                .first()
                .map_or_else(String::new, |file| file.relative_path.clone()),
            violation: AssetIdViolation::Empty,
        });
    }
    Ok(())
}

/// The role an agent occupies in a canonical team.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentRole {
    /// The agent that leads the team.
    Leader,
    /// An agent that follows the team leader.
    Follower,
}

/// A canonical team definition identified by its leader.
#[derive(Debug, Clone)]
pub struct TeamDefinition {
    /// The stable identity of the team leader.
    pub leader: AgentId,

    /// Team followers in canonical order.
    pub followers: Vec<AgentId>,

    /// Skills assigned to the team in canonical order.
    pub skills: Vec<SkillId>,

    /// Registry version for this team.
    pub version: Version,
}

/// Static metadata for a compiled canonical team.
#[derive(Debug, Clone)]
pub struct TeamSummary {
    /// The stable identity of the team leader.
    pub leader: AgentId,

    /// Team followers in canonical order.
    pub followers: Vec<AgentId>,

    /// Skills assigned to the team in canonical order.
    pub skills: Vec<SkillId>,

    /// Registry version for this team.
    pub version: Version,
}

/// One file in a canonical skill bundle.
#[derive(Debug, Clone)]
pub struct SkillBundleFile {
    /// Destination-safe path relative to the skill directory.
    pub relative_path: &'static str,

    /// Compile-time embedded file contents.
    pub contents: &'static str,
}

impl SkillBundleFile {
    /// Creates a bundle file after validating its relative destination path.
    pub fn new(relative_path: &'static str, contents: &'static str) -> Result<Self, Error> {
        validate_relative_path(relative_path)?;
        Ok(Self {
            relative_path,
            contents,
        })
    }
}

/// A compile-time embedded canonical skill definition.
#[derive(Debug, Clone)]
pub struct SkillDefinition {
    /// Stable skill identity.
    pub id: SkillId,

    /// Registry version for this skill.
    pub version: Version,

    /// Human-readable skill name.
    pub name: &'static str,

    /// Canonical trigger-oriented skill description.
    pub description: &'static str,

    /// Skill bundle files, with `SKILL.md` required as the first item.
    pub bundle: Vec<SkillBundleFile>,
}

impl SkillDefinition {
    /// Creates a skill definition with the canonical `SKILL.md` entry first.
    pub fn new(
        id: SkillId,
        version: Version,
        name: &'static str,
        description: &'static str,
        bundle: Vec<SkillBundleFile>,
    ) -> Result<Self, Error> {
        let definition = Self {
            id,
            version,
            name,
            description,
            bundle,
        };
        definition.validate()?;
        Ok(definition)
    }

    /// Validates bundle paths and the required first `SKILL.md` file.
    pub fn validate(&self) -> Result<(), Error> {
        for file in &self.bundle {
            validate_relative_path(file.relative_path)?;
        }

        if self.bundle.first().map(|file| file.relative_path) != Some("SKILL.md") {
            return Err(Error::InvalidAssetId {
                asset_id: self
                    .bundle
                    .first()
                    .map_or_else(String::new, |file| file.relative_path.to_owned()),
                violation: AssetIdViolation::Empty,
            });
        }

        Ok(())
    }
}

/// A compile-time embedded canonical command definition.
#[derive(Debug, Clone)]
pub struct CommandDefinition {
    /// Stable command identity.
    pub id: CommandId,

    /// Registry version for this command.
    pub version: Version,

    /// Human-readable command name.
    pub name: &'static str,

    /// Human-readable asset description.
    pub description: &'static str,

    /// Compile-time embedded command body.
    pub body: &'static str,
}

/// Static metadata for a compiled agent registry entry.
#[derive(Debug, Clone)]
pub struct AgentSummary {
    /// Stable agent identity.
    pub id: AgentId,

    /// Registry version for this agent.
    pub version: Version,

    /// The role this agent occupies in canonical teams.
    pub role: AgentRole,

    /// Human-readable agent name.
    pub name: &'static str,

    /// Human-readable agent job title.
    pub job_title: &'static str,

    /// Human-readable agent description.
    pub description: &'static str,
}

/// Static metadata for a compiled skill registry entry.
#[derive(Debug, Clone)]
pub struct SkillSummary {
    /// Stable skill identity.
    pub id: SkillId,

    /// Registry version for this skill.
    pub version: Version,

    /// Human-readable skill name.
    pub name: &'static str,

    /// Canonical trigger-oriented skill description.
    pub description: &'static str,
}

/// Static metadata for a compiled command registry entry.
#[derive(Debug, Clone)]
pub struct CommandSummary {
    /// Stable command identity.
    pub id: CommandId,

    /// Registry version for this command.
    pub version: Version,

    /// Human-readable command name.
    pub name: &'static str,

    /// Human-readable command description.
    pub description: &'static str,
}

macro_rules! asset_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String")]
        pub struct $name(String);

        impl $name {
            /// Creates an ID after validating its destination safety.
            pub fn new(value: impl Into<String>) -> Result<Self, Error> {
                Self::try_from(value.into())
            }

            /// Returns this ID's normalized relative destination segment.
            #[must_use]
            pub fn normalized_destination(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = Error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                let violation = if value.is_empty() {
                    Some(AssetIdViolation::Empty)
                } else if value.chars().any(char::is_control) {
                    Some(AssetIdViolation::ControlCharacter)
                } else if value == "." || value == ".." {
                    Some(AssetIdViolation::DotSegment)
                } else if Path::new(&value).is_absolute() {
                    Some(AssetIdViolation::AbsoluteOrRooted)
                } else if {
                    let bytes = value.as_bytes();
                    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
                } {
                    Some(AssetIdViolation::PathPrefix)
                } else if value.contains('/') || value.contains('\\') {
                    Some(AssetIdViolation::Separator)
                } else {
                    None
                };

                if let Some(violation) = violation {
                    return Err(Error::InvalidAssetId {
                        asset_id: value,
                        violation,
                    });
                }
                Ok(Self(value))
            }
        }

        impl TryFrom<&str> for $name {
            type Error = Error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::try_from(value.to_owned())
            }
        }
    };
}

asset_id!(AgentId, "A safe canonical agent destination identity.");
asset_id!(SkillId, "A safe canonical skill destination identity.");
asset_id!(CommandId, "A safe canonical command destination identity.");

fn validate_relative_path(relative_path: &str) -> Result<(), Error> {
    let bytes = relative_path.as_bytes();
    let has_path_prefix = bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':';
    let invalid = if relative_path.is_empty() {
        Some(AssetIdViolation::Empty)
    } else if relative_path.chars().any(char::is_control) {
        Some(AssetIdViolation::ControlCharacter)
    } else if Path::new(relative_path).is_absolute() {
        Some(AssetIdViolation::AbsoluteOrRooted)
    } else if has_path_prefix {
        Some(AssetIdViolation::PathPrefix)
    } else if relative_path.contains('\\') || relative_path.split('/').any(str::is_empty) {
        Some(AssetIdViolation::Separator)
    } else if relative_path
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        Some(AssetIdViolation::DotSegment)
    } else {
        None
    };

    invalid.map_or(Ok(()), |violation| {
        Err(Error::InvalidAssetId {
            asset_id: relative_path.to_owned(),
            violation,
        })
    })
}

/// Why an asset ID cannot safely form a relative destination segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetIdViolation {
    /// The ID is empty.
    Empty,
    /// The ID contains a destination separator.
    Separator,
    /// The ID is `.` or `..`.
    DotSegment,
    /// The ID is absolute or rooted.
    AbsoluteOrRooted,
    /// The ID has a platform path prefix.
    PathPrefix,
    /// The ID contains a NUL or other control character.
    ControlCharacter,
}

impl fmt::Display for AssetIdViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::Empty => "asset ID must not be empty",
            Self::Separator => "asset ID must be a single destination segment",
            Self::DotSegment => "asset ID must not be `.` or `..`",
            Self::AbsoluteOrRooted => "asset ID must not be absolute or rooted",
            Self::PathPrefix => "asset ID must not contain a platform path prefix",
            Self::ControlCharacter => "asset ID must not contain control characters",
        };

        formatter.write_str(reason)
    }
}

/// Failures produced while validating application-supplied asset definitions.
#[derive(Debug, Error)]
pub enum Error {
    /// An asset identity cannot safely name a destination segment.
    #[error("invalid asset id {asset_id:?}: {violation}")]
    InvalidAssetId {
        asset_id: String,
        violation: AssetIdViolation,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_invalid_asset_id<T>(result: Result<T, Error>, asset_id: &str, violation: AssetIdViolation) {
        let error = match result {
            Ok(_) => panic!("asset ID should be rejected"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            Error::InvalidAssetId {
                asset_id: invalid_id,
                violation: actual_violation,
            } if invalid_id == asset_id && actual_violation == violation
        ));
    }

    #[test]
    fn asset_ids_accept_safe_single_segments_without_cosmetic_restrictions() {
        let safe_id = "Agent name_42.@example";

        assert_eq!(AgentId::new(safe_id).unwrap().normalized_destination(), safe_id);
        assert_eq!(SkillId::try_from(safe_id).unwrap().as_ref(), safe_id);
        assert_eq!(CommandId::try_from(safe_id.to_owned()).unwrap().to_string(), safe_id);
    }

    #[test]
    fn asset_ids_reject_unsafe_destination_segments_with_typed_context() {
        let cases = [
            ("", AssetIdViolation::Empty),
            ("/", AssetIdViolation::AbsoluteOrRooted),
            ("agent\\id", AssetIdViolation::Separator),
            (".", AssetIdViolation::DotSegment),
            ("..", AssetIdViolation::DotSegment),
            ("/absolute", AssetIdViolation::AbsoluteOrRooted),
            ("C:prefix", AssetIdViolation::PathPrefix),
            ("agent\0id", AssetIdViolation::ControlCharacter),
            ("agent\nid", AssetIdViolation::ControlCharacter),
        ];

        for (asset_id, violation) in cases {
            assert_invalid_asset_id(AgentId::try_from(asset_id), asset_id, violation);
            assert_invalid_asset_id(SkillId::try_from(asset_id), asset_id, violation);
            assert_invalid_asset_id(CommandId::try_from(asset_id), asset_id, violation);
        }
    }
}
