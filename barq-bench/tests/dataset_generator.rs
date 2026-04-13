use barq_bench::dataset::{generate_dataset, DatasetConfig, DatasetError};

#[test]
fn same_seed_produces_same_dataset() {
    let config = DatasetConfig {
        seed: 42,
        count: 5,
        dimension: 3,
    };

    let first = generate_dataset(&config).expect("dataset should be generated");
    let second = generate_dataset(&config).expect("dataset should be generated");

    assert_eq!(first, second);
}

#[test]
fn dimension_and_count_are_respected() {
    let config = DatasetConfig {
        seed: 7,
        count: 4,
        dimension: 6,
    };

    let generated = generate_dataset(&config).expect("dataset should be generated");

    assert_eq!(generated.len(), config.count);
    assert!(generated
        .iter()
        .all(|record| record.values.len() == config.dimension));
}

#[test]
fn invalid_parameters_are_rejected() {
    let zero_count = DatasetConfig {
        seed: 1,
        count: 0,
        dimension: 8,
    };
    let zero_dimension = DatasetConfig {
        seed: 1,
        count: 8,
        dimension: 0,
    };

    assert_eq!(generate_dataset(&zero_count), Err(DatasetError::ZeroCount));
    assert_eq!(
        generate_dataset(&zero_dimension),
        Err(DatasetError::ZeroDimension)
    );
}
