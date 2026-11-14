//! The committed migration state machine: deterministic rejections and the committed
//! activation of a new ownership epoch.
use rovqet_metadata_raft::sm::MetadataState;
use rovqet_metadata_raft::types::{
    FenceProof, MetadataCommand, MetadataCommandResponse, MigrationStage,
};

const MIGRATION: u128 = 0xfeed_0000_0000_0001;

fn node(id: u64) -> MetadataCommand {
    MetadataCommand::SetNode {
        node: id,
        address: format!("127.0.0.1:{id}"),
    }
}

fn placement(owner: u64, epoch: u64) -> MetadataCommand {
    MetadataCommand::SetPlacement {
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
        owner,
        ownership_epoch: epoch,
        lease_valid_for_ms: 60_000,
    }
}

fn begin(source: u64, target: u64, epoch: u64, next: u64) -> MetadataCommand {
    MetadataCommand::BeginMigration {
        migration_id: MIGRATION,
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
        source_node: source,
        target_node: target,
        source_incarnation: 77,
        original_epoch: epoch,
        next_epoch: next,
        // A source-only migration: the dependent-partition bundle is covered by
        // `migration_bundle.rs`.
        dependent_partitions: Vec::new(),
    }
}

fn advance(stage: MigrationStage) -> MetadataCommand {
    MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage,
        transfer_watermark: Some(10),
        manifest_id: Some("manifest-1".into()),
        fence_proof: None,
        target_ready: false,
        dependencies: Vec::new(),
    }
}

fn fence_proof() -> FenceProof {
    FenceProof {
        migration_id: MIGRATION,
        old_owner: 1,
        old_epoch: 5,
        fence_offset: 42,
        fence_digest: "deadbeef".into(),
        manifest_generation: 3,
    }
}

fn state_with_placement() -> MetadataState {
    let mut state = MetadataState::default();
    assert!(matches!(
        state.apply(&node(1)),
        MetadataCommandResponse::Applied { .. }
    ));
    assert!(matches!(
        state.apply(&node(2)),
        MetadataCommandResponse::Applied { .. }
    ));
    assert!(matches!(
        state.apply(&placement(1, 5)),
        MetadataCommandResponse::Applied { .. }
    ));
    state
}

fn rejected(response: &MetadataCommandResponse) -> bool {
    matches!(response, MetadataCommandResponse::Rejected { .. })
}

#[test]
fn a_migration_requires_a_known_source_and_target() {
    let mut state = state_with_placement();
    assert!(rejected(&state.apply(&begin(1, 9, 5, 6))));
    assert!(rejected(&state.apply(&begin(9, 2, 5, 6))));
    assert!(rejected(&state.apply(&begin(1, 1, 5, 6))));
    assert!(rejected(&state.apply(&begin(1, 2, 5, 5))));
    assert!(rejected(&state.apply(&begin(1, 2, 0, 6))));
}

#[test]
fn a_migration_with_a_stale_expected_epoch_is_rejected() {
    let mut state = state_with_placement();
    let response = state.apply(&begin(1, 2, 4, 6));
    assert!(
        rejected(&response),
        "a stale expected ownership epoch must be rejected: {response:?}"
    );
}

#[test]
fn a_migration_of_a_partition_that_is_not_owned_by_the_source_is_rejected() {
    let mut state = state_with_placement();
    assert!(rejected(&state.apply(&begin(2, 1, 5, 6))));
}

#[test]
fn a_second_migration_of_the_same_partition_is_rejected() {
    let mut state = state_with_placement();
    assert!(!rejected(&state.apply(&begin(1, 2, 5, 6))));
    let other = MetadataCommand::BeginMigration {
        migration_id: MIGRATION + 1,
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
        source_node: 1,
        target_node: 2,
        source_incarnation: 77,
        original_epoch: 5,
        next_epoch: 6,
        dependent_partitions: Vec::new(),
    };
    let response = state.apply(&other);
    assert!(
        rejected(&response),
        "a concurrent migration of the same partition must be rejected: {response:?}"
    );
}

#[test]
fn the_same_migration_id_with_other_content_is_rejected() {
    let mut state = state_with_placement();
    assert!(!rejected(&state.apply(&begin(1, 2, 5, 6))));
    let conflicting = MetadataCommand::BeginMigration {
        migration_id: MIGRATION,
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
        source_node: 1,
        target_node: 3,
        source_incarnation: 77,
        original_epoch: 5,
        next_epoch: 6,
        dependent_partitions: Vec::new(),
    };
    assert!(rejected(&state.apply(&conflicting)));
}

#[test]
fn the_same_migration_command_twice_is_idempotent() {
    let mut state = state_with_placement();
    let first = state.apply(&begin(1, 2, 5, 6));
    let revision = match first {
        MetadataCommandResponse::Applied { revision } => revision,
        other => panic!("expected an applied command: {other:?}"),
    };
    let second = state.apply(&begin(1, 2, 5, 6));
    assert_eq!(
        second,
        MetadataCommandResponse::Applied { revision },
        "a repeated command must not create a new revision"
    );
}

#[test]
fn a_stage_may_not_be_skipped() {
    let mut state = state_with_placement();
    assert!(!rejected(&state.apply(&begin(1, 2, 5, 6))));
    let response = state.apply(&advance(MigrationStage::FenceOld));
    assert!(
        rejected(&response),
        "a stage may not be skipped: {response:?}"
    );
    assert!(!rejected(&state.apply(&advance(MigrationStage::CatchUp))));
}

#[test]
fn the_fence_needs_durable_evidence_that_matches_the_migration() {
    let mut state = state_with_placement();
    assert!(!rejected(&state.apply(&begin(1, 2, 5, 6))));
    assert!(!rejected(&state.apply(&advance(MigrationStage::CatchUp))));

    let without_proof = state.apply(&advance(MigrationStage::FenceOld));
    assert!(
        rejected(&without_proof),
        "a fence without durable evidence must be rejected: {without_proof:?}"
    );

    let mut foreign = fence_proof();
    foreign.old_owner = 2;
    let wrong_owner = MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage: MigrationStage::FenceOld,
        transfer_watermark: Some(10),
        manifest_id: Some("manifest-1".into()),
        fence_proof: Some(foreign),
        target_ready: false,
        dependencies: Vec::new(),
    };
    assert!(rejected(&state.apply(&wrong_owner)));

    let mut foreign = fence_proof();
    foreign.migration_id = MIGRATION + 1;
    let wrong_migration = MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage: MigrationStage::FenceOld,
        transfer_watermark: Some(10),
        manifest_id: Some("manifest-1".into()),
        fence_proof: Some(foreign),
        target_ready: false,
        dependencies: Vec::new(),
    };
    assert!(rejected(&state.apply(&wrong_migration)));
}

#[test]
fn the_activation_needs_a_fence_a_verified_transfer_and_target_readiness() {
    let mut state = state_with_placement();
    assert!(!rejected(&state.apply(&begin(1, 2, 5, 6))));
    assert!(!rejected(&state.apply(&advance(MigrationStage::CatchUp))));

    let without_fence = state.apply(&advance(MigrationStage::ActivateNew));
    assert!(
        rejected(&without_fence),
        "an activation without fence evidence must be rejected: {without_fence:?}"
    );

    let fenced = MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage: MigrationStage::FenceOld,
        transfer_watermark: Some(10),
        manifest_id: Some("manifest-1".into()),
        fence_proof: Some(fence_proof()),
        target_ready: false,
        dependencies: Vec::new(),
    };
    assert!(!rejected(&state.apply(&fenced)));

    let without_readiness = state.apply(&advance(MigrationStage::ActivateNew));
    assert!(
        rejected(&without_readiness),
        "an activation without target readiness must be rejected: {without_readiness:?}"
    );

    let without_watermark = MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage: MigrationStage::ActivateNew,
        transfer_watermark: None,
        manifest_id: Some("manifest-2".into()),
        fence_proof: None,
        target_ready: true,
        dependencies: Vec::new(),
    };
    assert!(
        rejected(&state.apply(&without_watermark)),
        "an activation without a verified transfer must be rejected: {without_watermark:?}"
    );
}

#[test]
fn the_committed_activation_moves_the_placement_to_the_new_owner_and_epoch() {
    let mut state = state_with_placement();
    assert!(!rejected(&state.apply(&begin(1, 2, 5, 6))));
    assert!(!rejected(&state.apply(&advance(MigrationStage::CatchUp))));
    let fenced = MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage: MigrationStage::FenceOld,
        transfer_watermark: Some(10),
        manifest_id: Some("manifest-1".into()),
        fence_proof: Some(fence_proof()),
        target_ready: false,
        dependencies: Vec::new(),
    };
    assert!(!rejected(&state.apply(&fenced)));

    // Until the activation is committed the old owner stays authoritative.
    let placement = state.placement(1, 3, 0).expect("placement");
    assert_eq!(placement.owner, 1);
    assert_eq!(placement.ownership_epoch, 5);

    let activation = MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage: MigrationStage::ActivateNew,
        transfer_watermark: Some(12),
        manifest_id: Some("manifest-2".into()),
        fence_proof: None,
        target_ready: true,
        dependencies: Vec::new(),
    };
    assert!(!rejected(&state.apply(&activation)));
    let placement = state.placement(1, 3, 0).expect("placement");
    assert_eq!(placement.owner, 2, "the new owner must own the partition");
    assert_eq!(
        placement.ownership_epoch, 6,
        "the new ownership epoch must be used"
    );
}

#[test]
fn the_committed_record_keeps_the_evidence_of_every_stage() {
    let mut state = state_with_placement();
    assert!(!rejected(&state.apply(&begin(1, 2, 5, 6))));
    assert!(!rejected(&state.apply(&advance(MigrationStage::CatchUp))));
    let fenced = MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage: MigrationStage::FenceOld,
        transfer_watermark: Some(10),
        manifest_id: Some("manifest-1".into()),
        fence_proof: Some(fence_proof()),
        target_ready: false,
        dependencies: Vec::new(),
    };
    assert!(!rejected(&state.apply(&fenced)));
    let record = state.migrations.get(&MIGRATION).expect("migration record");
    assert_eq!(record.stage, MigrationStage::FenceOld);
    assert_eq!(record.source_node, 1);
    assert_eq!(record.target_node, 2);
    assert_eq!(record.source_incarnation, 77);
    assert_eq!(record.original_epoch, 5);
    assert_eq!(record.next_epoch, 6);
    assert_eq!(record.transfer_watermark, Some(10));
    assert_eq!(record.manifest_id.as_deref(), Some("manifest-1"));
    assert!(record.fence_proof.is_some());
    assert!(!record.target_ready);
    assert!(record.dependent_partitions.is_empty());
}
