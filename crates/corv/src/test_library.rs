//! Injected catalog for materialization and lifecycle tests, without application prompt content.

use std::sync::LazyLock;

use semver::Version;

use super::library::{
    AgentDefinition, AgentId, AgentRole, AssetLibrary, SkillBundleFile, SkillDefinition, SkillId, TeamDefinition,
};

static AGENTS: LazyLock<Vec<AgentDefinition>> = LazyLock::new(|| {
    [
        ("chief-of-staff", AgentRole::Leader),
        ("equity-analyst", AgentRole::Leader),
        ("sector-analyst", AgentRole::Leader),
        ("sluice-customizer", AgentRole::Leader),
        ("equity-auditor", AgentRole::Follower),
        ("generalist-reviewer", AgentRole::Follower),
        ("sector-auditor", AgentRole::Follower),
    ]
    .into_iter()
    .map(|(id, role)| AgentDefinition {
        id: AgentId::new(id).expect("fixture id"),
        version: Version::new(1, 0, 0),
        role,
        name: "Test agent",
        job_title: "Test role",
        description: "Test asset",
        body: "Test agent instructions.\n",
    })
    .collect()
});

static SKILLS: LazyLock<Vec<SkillDefinition>> = LazyLock::new(|| {
    [
        "financial-data",
        "forensics",
        "valuation",
        "market-methodology",
        "equity-outlook",
        "flow-builder",
    ]
    .into_iter()
    .map(|id| {
        SkillDefinition::new(
            SkillId::new(id).expect("fixture id"),
            Version::new(1, 0, 0),
            "Test skill",
            "Test skill description",
            vec![
                SkillBundleFile::new("SKILL.md", "Test skill instructions.\n").expect("fixture path"),
                SkillBundleFile::new("scripts/helper.ts", "export const value = 1;\n").expect("fixture path"),
            ],
        )
        .expect("fixture skill")
    })
    .collect()
});

static TEAMS: LazyLock<Vec<TeamDefinition>> = LazyLock::new(|| {
    [
        ("chief-of-staff", &[][..], &[][..]),
        (
            "equity-analyst",
            &["equity-auditor", "generalist-reviewer"][..],
            &[
                "financial-data",
                "forensics",
                "valuation",
                "market-methodology",
                "equity-outlook",
            ][..],
        ),
        (
            "sector-analyst",
            &["sector-auditor"][..],
            &["financial-data", "valuation"][..],
        ),
        ("sluice-customizer", &[][..], &["flow-builder"][..]),
    ]
    .into_iter()
    .map(|(leader, followers, skills)| TeamDefinition {
        leader: AgentId::new(leader).expect("fixture leader"),
        followers: followers
            .iter()
            .map(|id| AgentId::new(*id).expect("fixture follower"))
            .collect(),
        skills: skills
            .iter()
            .map(|id| SkillId::new(*id).expect("fixture skill id"))
            .collect(),
        version: Version::new(1, 0, 0),
    })
    .collect()
});

pub(super) fn library() -> AssetLibrary {
    AssetLibrary::new(&AGENTS, &SKILLS, &[], &TEAMS)
}
