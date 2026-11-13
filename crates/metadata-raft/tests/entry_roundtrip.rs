use openraft::LeaderId;
use openraft::log_id::LogId;
use openraft::{Entry, EntryPayload};
use rovqet_metadata_raft::types::{MetadataCommand, MetadataRaftTypeConfig};

#[test]
fn entry_round_trip_keeps_its_log_id() {
    let entry = Entry::<MetadataRaftTypeConfig> {
        log_id: LogId::new(LeaderId::new(2, 1), 5),
        payload: EntryPayload::Normal(MetadataCommand::SetNode {
            node: 3,
            address: "127.0.0.1:1".into(),
        }),
    };
    let json = serde_json::to_string(&entry).expect("serialize");
    println!("json: {json}");
    let back: Entry<MetadataRaftTypeConfig> = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back.log_id, entry.log_id);
}
