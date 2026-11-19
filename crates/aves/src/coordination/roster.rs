//! Exact deterministic validator weights from committed member allocations.

use std::collections::BTreeSet;

use num_integer::Integer;

use super::abci::CommittedState;
use super::authority::current_member_authority;
use super::types::{ChainHalted, CoordinationError, EffectiveValidator, PublicKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RosterOutcome {
    Validators(Vec<(PublicKey, u64)>),
    NoEligibleValidators,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RosterError {
    InvalidAllocation,
    Overflow,
}

/// Derives the next roster without rounding or retaining revoked machine keys.
pub(super) fn derive_roster(state: &CommittedState) -> Result<RosterOutcome, RosterError> {
    let mut fractions = Vec::new();
    let mut initial_roster = Vec::new();
    let owner_has_power = state.owner.as_ref().is_some_and(|owner| {
        state
            .member_power
            .get(&owner.user)
            .is_some_and(|power| power.revision > 0)
    });
    if !owner_has_power {
        let mut initial = state.initial_roster.clone();
        initial.sort_by_key(|(key, _)| *key);
        for (key, weight) in initial {
            let revoked = state.owner.as_ref().is_some_and(|owner| {
                state
                    .users
                    .get(&owner.user)
                    .is_some_and(|user| user.revoked.is_some())
                    || state.machines.values().any(|machine| {
                        machine
                            .bindings
                            .iter()
                            .any(|binding| binding.user == owner.user && binding.machine_key == key)
                            && machine
                                .unbindings
                                .iter()
                                .any(|unbinding| unbinding.user == owner.user)
                    })
                    || state
                        .member_authorities
                        .get(&owner.user)
                        .is_some_and(|authority| {
                            !authority
                                .certified_machines
                                .iter()
                                .any(|machine| machine.machine_key == key)
                        })
            });
            if revoked {
                continue;
            }
            initial_roster.push((key, weight));
        }
    }
    for (member, power) in &state.member_power {
        if power.total == 0 {
            continue;
        }
        let Some(authority) = current_member_authority(state, *member) else {
            continue;
        };
        let Some(split) = state.member_splits.get(member) else {
            continue;
        };
        let mut parts = split.splits.clone();
        parts.sort_by_key(|part| part.machine);
        let mut seen = BTreeSet::new();
        let mut eligible = Vec::new();
        for part in parts {
            if part.parts == 0 || !seen.insert(part.machine) {
                return Err(RosterError::InvalidAllocation);
            }
            if let Some(machine) = authority
                .certified_machines
                .iter()
                .find(|machine| machine.machine == part.machine)
            {
                eligible.push((machine.machine_key, part.parts));
            }
        }
        let denominator = eligible.iter().try_fold(0_u128, |sum, (_, part)| {
            sum.checked_add(u128::from(*part))
                .ok_or(RosterError::Overflow)
        })?;
        for (key, part) in eligible {
            let numerator = u128::from(power.total)
                .checked_mul(u128::from(part))
                .ok_or(RosterError::Overflow)?;
            fractions.push((key, numerator, denominator));
        }
    }
    if fractions.is_empty() {
        if initial_roster.is_empty() {
            return Ok(RosterOutcome::NoEligibleValidators);
        }
        let mut keys = BTreeSet::new();
        let mut total = 0_u64;
        for (key, weight) in &initial_roster {
            if *weight == 0 || !keys.insert(*key) {
                return Err(RosterError::InvalidAllocation);
            }
            total = total.checked_add(*weight).ok_or(RosterError::Overflow)?;
        }
        if total > u64::MAX / 3 {
            return Err(RosterError::Overflow);
        }
        return Ok(RosterOutcome::Validators(initial_roster));
    }
    fractions.extend(
        initial_roster
            .iter()
            .map(|(key, weight)| (*key, u128::from(*weight), 1_u128)),
    );
    fractions.sort_by_key(|(key, _, _)| *key);
    normalize(fractions)
}

/// Records the roster effective after a committed height, preserving older heights.
pub(super) fn record_next_roster(state: &mut CommittedState, height: u64) -> Result<(), RosterError> {
    let next = height.checked_add(1).ok_or(RosterError::Overflow)?;
    let roster = match derive_roster(state) {
        Ok(RosterOutcome::Validators(roster)) => roster,
        Ok(RosterOutcome::NoEligibleValidators) | Err(_) => Vec::new(),
    };
    let mut owners = Vec::with_capacity(roster.len());
    for (key, _) in &roster {
        let owner = state
            .owner
            .as_ref()
            .filter(|owner| {
                !state
                    .member_power
                    .get(&owner.user)
                    .is_some_and(|power| power.revision > 0)
                    && state
                        .initial_roster
                        .iter()
                        .any(|(initial, _)| initial == key)
            })
            .map(|owner| owner.user)
            .or_else(|| {
                state
                    .member_power
                    .iter()
                    .filter(|(_, power)| power.total > 0)
                    .find_map(|(member, _)| {
                        current_member_authority(state, *member)
                            .filter(|authority| {
                                authority.certified_machines.iter().any(|machine| {
                                    machine.machine_key == *key
                                        && state.member_splits.get(member).is_some_and(|split| {
                                            split
                                                .splits
                                                .iter()
                                                .any(|part| part.machine == machine.machine)
                                        })
                                })
                            })
                            .map(|_| *member)
                    })
            })
            .ok_or(RosterError::InvalidAllocation)?;
        owners.push((*key, owner));
    }
    state.roster_owners.insert(next, owners);
    state.roster_history.insert(next, roster);
    Ok(())
}

/// Reads the stored roster and its ownership at one exact height.
pub(super) fn effective_roster(
    state: &CommittedState,
    requested: Option<u64>,
) -> Result<(u64, Vec<EffectiveValidator>, Option<ChainHalted>), CoordinationError> {
    let current = state
        .height
        .0
        .checked_add(1)
        .ok_or(CoordinationError::InvalidValidatorSet)?;
    let height = requested.unwrap_or(current);
    let roster = state
        .roster_history
        .get(&height)
        .ok_or(CoordinationError::RosterHeightUnavailable { height })?;
    let owners = state
        .roster_owners
        .get(&height)
        .ok_or(CoordinationError::RosterHeightUnavailable { height })?;
    if owners.len() != roster.len() {
        return Err(CoordinationError::RosterHeightUnavailable { height });
    }
    let validators = roster
        .iter()
        .zip(owners)
        .map(|((key, power), (owner_key, member))| {
            if key != owner_key {
                return Err(CoordinationError::RosterHeightUnavailable { height });
            }
            Ok(EffectiveValidator {
                machine_key: *key,
                voting_power: *power,
                member: *member,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let halted = state
        .roster_history
        .get(&current)
        .filter(|roster| roster.is_empty())
        .map(|_| ChainHalted::NoEligibleValidators);
    Ok((height, validators, halted))
}

fn normalize(fractions: Vec<(PublicKey, u128, u128)>) -> Result<RosterOutcome, RosterError> {
    if fractions.is_empty() {
        return Ok(RosterOutcome::NoEligibleValidators);
    }
    let mut denominator = 1_u128;
    let mut keys = BTreeSet::new();
    for (key, numerator, divisor) in &fractions {
        if *numerator == 0 || *divisor == 0 || !keys.insert(*key) {
            return Err(RosterError::InvalidAllocation);
        }
        let weight = *numerator / *divisor;
        if weight > u128::from(u64::MAX / 3) || (weight == u128::from(u64::MAX / 3) && *numerator % *divisor != 0) {
            return Err(RosterError::Overflow);
        }
        let reduced = *divisor / numerator.gcd(divisor);
        (denominator / denominator.gcd(&reduced))
            .checked_mul(reduced)
            .ok_or(RosterError::Overflow)?;
        denominator = denominator.lcm(&reduced);
    }
    let mut weights = Vec::with_capacity(fractions.len());
    let mut factor = 0;
    for (key, numerator, divisor) in fractions {
        let gcd = numerator.gcd(&divisor);
        let weight = (numerator / gcd)
            .checked_mul(denominator / (divisor / gcd))
            .ok_or(RosterError::Overflow)?;
        factor = factor.gcd(&weight);
        weights.push((key, weight));
    }
    let mut total = 0_u64;
    let mut roster = Vec::with_capacity(weights.len());
    for (key, weight) in weights {
        let weight = u64::try_from(weight / factor).map_err(|_| RosterError::Overflow)?;
        total = total.checked_add(weight).ok_or(RosterError::Overflow)?;
        roster.push((key, weight));
    }
    if total > u64::MAX / 3 {
        return Err(RosterError::Overflow);
    }
    Ok(RosterOutcome::Validators(roster))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::transaction::{ApplyOutcome, PendingTransaction, RejectionReason};
    use crate::coordination::types::{
        DomainId, EncKey, Height, MachineBound, MachineId, MachineRecord, MachineUnbound, MemberAuthority,
        MemberIdentityProof, MemberPowerRecord, MemberRecord, MemberStatus, MemberValidatorSplitChanged,
        MemberValidatorSplitRecord, SignedBody, UserId, ValidatorSplitPart,
    };

    fn member(state: &mut CommittedState, id: u8, total: u64) {
        let user = UserId([id; 32]);
        let machine = MachineId([id; 32]);
        let key = PublicKey([id; 32]);
        state.members.insert(
            user,
            MemberRecord {
                member: user,
                status: MemberStatus::Active,
                revision: 0,
                root: key,
                proof: MemberIdentityProof {
                    source_domain: DomainId([9; 32]),
                    certificate: SignedBody {
                        signer: key,
                        sig: [0; 64],
                        body: MachineBound {
                            user,
                            machine,
                            machine_key: key,
                            sonya_key: key,
                            enc_key: EncKey([id; 32]),
                            enc_epoch: 1,
                        },
                    },
                },
            },
        );
        state.member_power.insert(
            user,
            MemberPowerRecord {
                member: user,
                revision: 1,
                total,
            },
        );
        state.member_splits.insert(
            user,
            MemberValidatorSplitRecord {
                member: user,
                revision: 1,
                splits: vec![ValidatorSplitPart { machine, parts: 1 }],
            },
        );
    }

    #[test]
    fn removed_member_cannot_remain_a_validator() {
        let mut state = CommittedState::default();
        member(&mut state, 1, 3);
        assert_eq!(
            derive_roster(&state),
            Ok(RosterOutcome::Validators(vec![(PublicKey([1; 32]), 1)]))
        );
        state.members.get_mut(&UserId([1; 32])).unwrap().status = MemberStatus::Removed;
        assert_eq!(derive_roster(&state), Ok(RosterOutcome::NoEligibleValidators));
        record_next_roster(&mut state, 1).unwrap();
        assert!(state.roster_history[&2].is_empty());
    }

    #[test]
    fn rejected_revoked_split_does_not_change_the_roster() {
        let mut state = CommittedState::default();
        member(&mut state, 1, 3);
        let user = UserId([1; 32]);
        let revoked = MachineId([2; 32]);
        let mut binding = state.members[&user].proof.certificate.body.clone();
        binding.machine = revoked;
        binding.machine_key = PublicKey([2; 32]);
        state.machines.insert(
            revoked,
            MachineRecord {
                machine: revoked,
                registrations: Vec::new(),
                bindings: vec![binding],
                unbindings: vec![MachineUnbound {
                    user,
                    machine: revoked,
                    effective_at: 1,
                    compromised_from: None,
                }],
            },
        );
        let before = derive_roster(&state).unwrap();
        let split = MemberValidatorSplitChanged {
            domain: DomainId([9; 32]),
            member: user,
            previous_revision: 1,
            revision: 2,
            splits: vec![ValidatorSplitPart {
                machine: revoked,
                parts: 1,
            }],
        };
        let tx = PendingTransaction::MemberValidatorSplitChanged(SignedBody {
            body: split,
            signer: PublicKey([1; 32]),
            sig: [0; 64],
        });
        assert_eq!(
            tx.apply(&mut state, Height(1)).unwrap(),
            ApplyOutcome::Rejected(RejectionReason::InvalidPayload)
        );
        assert_eq!(derive_roster(&state).unwrap(), before);
    }

    #[test]
    fn changes_take_effect_next_height_and_history_is_retained() {
        let mut state = CommittedState::default();
        member(&mut state, 1, 3);
        member(&mut state, 2, 5);
        record_next_roster(&mut state, 0).unwrap();
        let old = state.roster_history[&1].clone();
        state.member_power.get_mut(&UserId([2; 32])).unwrap().total = 7;
        record_next_roster(&mut state, 1).unwrap();
        assert_eq!(state.roster_history[&1], old);
        assert_eq!(
            state.roster_history[&2],
            vec![(PublicKey([1; 32]), 3), (PublicKey([2; 32]), 7)]
        );
    }

    #[test]
    fn exact_common_denominator_and_reduction() {
        let result = normalize(vec![
            (PublicKey([1; 32]), 3, 3),
            (PublicKey([2; 32]), 6, 3),
            (PublicKey([3; 32]), 5, 3),
            (PublicKey([4; 32]), 10, 3),
        ])
        .unwrap();
        assert_eq!(
            result,
            RosterOutcome::Validators(vec![
                (PublicKey([1; 32]), 3),
                (PublicKey([2; 32]), 6),
                (PublicKey([3; 32]), 5),
                (PublicKey([4; 32]), 10),
            ])
        );
    }

    #[test]
    fn derives_two_member_splits_in_canonical_order() {
        let mut state = CommittedState::default();
        for (id, total) in [(1, 3), (3, 5)] {
            member(&mut state, id, total);
            let user = UserId([id; 32]);
            let first = state.members[&user].proof.certificate.body.clone();
            let mut second = first.clone();
            second.machine = MachineId([id + 1; 32]);
            second.machine_key = PublicKey([id + 1; 32]);
            state.member_authorities.insert(
                user,
                MemberAuthority {
                    root: first.machine_key,
                    certified_machines: vec![second.clone(), first],
                    proof_revision: 1,
                    history: Vec::new(),
                },
            );
            state
                .member_splits
                .get_mut(&user)
                .unwrap()
                .splits
                .push(ValidatorSplitPart {
                    machine: second.machine,
                    parts: 2,
                });
        }
        assert_eq!(
            derive_roster(&state),
            Ok(RosterOutcome::Validators(vec![
                (PublicKey([1; 32]), 3),
                (PublicKey([2; 32]), 6),
                (PublicKey([3; 32]), 5),
                (PublicKey([4; 32]), 10),
            ]))
        );
    }

    #[test]
    fn rejects_duplicates_and_overflow() {
        let key = PublicKey([1; 32]);
        assert_eq!(
            normalize(vec![(key, 1, 1), (key, 1, 1)]),
            Err(RosterError::InvalidAllocation)
        );
        assert_eq!(normalize(vec![(key, u128::MAX, 1)]), Err(RosterError::Overflow));
    }
}
