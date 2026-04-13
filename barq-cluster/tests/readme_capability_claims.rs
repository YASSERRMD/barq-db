use std::fs;
use std::path::PathBuf;

fn workspace_readme() -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("README.md");
    fs::read_to_string(root).expect("workspace README")
}

#[test]
fn readme_does_not_claim_raft_consensus() {
    let readme = workspace_readme();
    assert!(
        !readme.contains("Raft Consensus"),
        "README should not claim Raft consensus"
    );
    assert!(
        !readme.contains("replication and consensus"),
        "README should not claim consensus-backed replication"
    );
}

#[test]
fn readme_describes_routed_replication_honestly() {
    let readme = workspace_readme();
    assert!(
        readme.contains("Replicated multi-node deployments now route writes through per-shard Raft quorum commit"),
        "README should document the runtime quorum commit path"
    );
    assert!(
        readme.contains("Raft leader election"),
        "README should mention the deterministic consensus engine when it exists"
    );
    assert!(
        readme.contains("Single-replica multi-node deployments remain routed replication without quorum durability"),
        "README should keep the routed-replication fallback honest"
    );
}
