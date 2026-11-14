//! The committed bundle contract: a source and its dependent DLQ partition move as one
//! authoritative activation.
use rovqet_metadata_raft::sm::MetadataState;
use rovqet_metadata_raft::types::{
    DependentEvidence, DependentPartition, FenceProof, MetadataCommand, MetadataCommandResponse,
    MigrationStage,
};

const MIGRATION: u128 = 0xaaaa_bbbb_cccc_dddd;

fn node(id: u64) -> MetadataCommand {
    MetadataCommand::SetNode {
        node: id,
        address: format!("127.0.0.1:{id}"),
    }
}

fn placement(partition_id: u32, owner: u64, epoch: u64) -> MetadataCommand {
    MetadataCommand::SetPlacement {
        tenant_id: 1,
        topic_id: 3,
        partition_id,
        owner,
        ownership_epoch: epoch,
        lease_valid_for_ms: 600_000,
    }
}

fn state() -> MetadataState {
    let mut state = MetadataState::default();
    for command in [node(1), node(2), placement(0, 1, 7), placement(9, 1, 7)] {
        assert!(matches!(
            state.apply(&command),
            MetadataCommandResponse::Applied { .. }
        ));
    }
    state
}

fn begin() -> MetadataCommand {
    MetadataCommand::BeginMigration {
        migration_id: MIGRATION,
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
        source_node: 1,
        target_node: 2,
        source_incarnation: 42,
        original_epoch: 7,
        next_epoch: 8,
        dependent_partitions: vec![DependentPartition {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 9,
            durable_class: 1,
            incarnation: 43,
        }],
    }
}

fn proof(old_epoch: u64) -> FenceProof {
    FenceProof {
        migration_id: MIGRATION,
        old_owner: 1,
        old_epoch,
        fence_offset: 3,
        fence_digest: "d".into(),
        manifest_generation: 2,
    }
}

fn evidence(with_fence: bool, ready: bool) -> DependentEvidence {
    DependentEvidence {
        tenant_id: 1,
        topic_id: 3,
        partition_id: 9,
        incarnation: 43,
        original_epoch: 7,
        next_epoch: 8,
        transfer_watermark: Some(4),
        manifest_id: Some("dlq".into()),
        fence_proof: with_fence.then(|| proof(7)),
        ready,
    }
}

fn advance(stage: MigrationStage, dependencies: Vec<DependentEvidence>) -> MetadataCommand {
    MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage,
        transfer_watermark: Some(10),
        manifest_id: Some("src".into()),
        fence_proof: Some(proof(7)),
        target_ready: true,
        dependencies,
    }
}

fn rejected(response: &MetadataCommandResponse) -> bool {
    matches!(response, MetadataCommandResponse::Rejected { .. })
}

#[test]
fn the_bundle_activation_moves_both_placements_in_one_committed_step() {
    let mut state = state();
    assert!(!rejected(&state.apply(&begin())));
    assert!(!rejected(&state.apply(&advance(
        MigrationStage::CatchUp,
        vec![evidence(true, true)],
    ))));
    assert!(!rejected(&state.apply(&advance(
        MigrationStage::FenceOld,
        vec![evidence(true, true)],
    ))));

    // Before the activation both partitions are still owned by the source.
    assert_eq!(state.placement(1, 3, 0).expect("source").owner, 1);
    assert_eq!(state.placement(1, 3, 9).expect("dlq").owner, 1);

    assert!(!rejected(&state.apply(&advance(
        MigrationStage::ActivateNew,
        vec![evidence(true, true)],
    ))));

    // Both partitions moved, in the same committed entry.
    let source = state.placement(1, 3, 0).expect("source");
    let dlq = state.placement(1, 3, 9).expect("dlq");
    assert_eq!(source.owner, 2);
    assert_eq!(source.ownership_epoch, 8);
    assert_eq!(dlq.owner, 2, "the dependency must move with the source");
    assert_eq!(dlq.ownership_epoch, 8);
}

#[test]
fn the_bundle_fence_requires_the_fence_of_every_dependency() {
    let mut state = state();
    assert!(!rejected(&state.apply(&begin())));
    assert!(!rejected(&state.apply(&advance(
        MigrationStage::CatchUp,
        vec![evidence(true, true)],
    ))));
    // No evidence at all.
    let missing = state.apply(&advance(MigrationStage::FenceOld, Vec::new()));
    assert!(
        rejected(&missing),
        "a bundle fence without its dependency evidence must be refused: {missing:?}"
    );
    // Evidence without the dependency's durable fence.
    let unfenced = state.apply(&advance(
        MigrationStage::FenceOld,
        vec![evidence(false, true)],
    ));
    assert!(
        rejected(&unfenced),
        "a bundle fence without the dependency fence must be refused: {unfenced:?}"
    );
    // A fence proof of another epoch is refused.
    let mut foreign = evidence(true, true);
    foreign.fence_proof = Some(proof(6));
    assert!(rejected(
        &state.apply(&advance(MigrationStage::FenceOld, vec![foreign],))
    ));
}

#[test]
fn the_bundle_activation_requires_the_dependency_transfer_and_readiness() {
    let mut state = state();
    assert!(!rejected(&state.apply(&begin())));
    assert!(!rejected(&state.apply(&advance(
        MigrationStage::CatchUp,
        vec![evidence(true, true)],
    ))));
    assert!(!rejected(&state.apply(&advance(
        MigrationStage::FenceOld,
        vec![evidence(true, true)],
    ))));

    // Not ready: the source must not activate.
    let not_ready = state.apply(&advance(
        MigrationStage::ActivateNew,
        vec![evidence(true, false)],
    ));
    assert!(
        rejected(&not_ready),
        "an activation with an unready dependency must be refused: {not_ready:?}"
    );
    // No verified dependency transfer.
    let mut unverified = evidence(true, true);
    unverified.transfer_watermark = None;
    assert!(rejected(
        &state.apply(&advance(MigrationStage::ActivateNew, vec![unverified],))
    ));
    // Missing evidence.
    assert!(rejected(
        &state.apply(&advance(MigrationStage::ActivateNew, Vec::new(),))
    ));
    // The source never activated alone: the placement is unchanged.
    assert_eq!(state.placement(1, 3, 0).expect("source").owner, 1);
    assert_eq!(state.placement(1, 3, 9).expect("dlq").owner, 1);
}

#[test]
fn a_source_only_migration_is_unchanged() {
    let mut state = state();
    let mut command = begin();
    if let MetadataCommand::BeginMigration {
        dependent_partitions,
        ..
    } = &mut command
    {
        dependent_partitions.clear();
    }
    assert!(!rejected(&state.apply(&command)));
    assert!(!rejected(
        &state.apply(&advance(MigrationStage::CatchUp, Vec::new(),))
    ));
    assert!(!rejected(
        &state.apply(&advance(MigrationStage::FenceOld, Vec::new(),))
    ));
    assert!(!rejected(
        &state.apply(&advance(MigrationStage::ActivateNew, Vec::new(),))
    ));
    assert_eq!(state.placement(1, 3, 0).expect("source").owner, 2);
    // The unrelated partition never moved.
    assert_eq!(
        state.placement(1, 3, 9).expect("dlq").owner,
        1,
        "a source-only migration must not move an unrelated partition"
    );
}

#[test]
fn a_repeated_bundle_command_is_idempotent() {
    let mut state = state();
    assert!(!rejected(&state.apply(&begin())));
    let first = state.apply(&advance(
        MigrationStage::CatchUp,
        vec![evidence(true, true)],
    ));
    let revision = match first {
        MetadataCommandResponse::Applied { revision } => revision,
        other => panic!("expected an applied command: {other:?}"),
    };
    let second = state.apply(&advance(
        MigrationStage::CatchUp,
        vec![evidence(true, true)],
    ));
    assert_eq!(
        second,
        MetadataCommandResponse::Applied { revision },
        "a repeated bundle command must not create a new revision"
    );
    // Conflicting evidence for the same stage is refused.
    let mut conflicting = evidence(true, true);
    conflicting.manifest_id = Some("other".into());
    assert!(rejected(
        &state.apply(&advance(MigrationStage::CatchUp, vec![conflicting],))
    ));
}
