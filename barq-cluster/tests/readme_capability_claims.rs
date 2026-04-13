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
        readme.contains("routed replication"),
        "README should describe the current cluster capability honestly"
    );
    assert!(
        readme.contains("future work"),
        "README should keep consensus language in future-work framing only"
    );
}
