use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use thiserror::Error;

/// Supported metric kinds in the Barq observability catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// Static definition for an emitted metric.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricDefinition {
    pub name: String,
    pub kind: MetricKind,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

impl MetricDefinition {
    /// Creates a new metric definition with no unit or labels.
    pub fn new(name: impl Into<String>, kind: MetricKind, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind,
            description: description.into(),
            unit: None,
            labels: Vec::new(),
        }
    }

    /// Attaches a unit to the metric definition.
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// Attaches label keys to the metric definition.
    pub fn with_labels(mut self, labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.labels = labels.into_iter().map(Into::into).collect();
        self
    }
}

/// Duplicate registration error for conflicting metric definitions.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MetricsRegistryError {
    #[error("metric {name} is already registered with a different definition")]
    ConflictingDefinition { name: String },
}

/// Shared registry of known metric definitions for endpoint exposure and validation.
#[derive(Clone, Debug, Default)]
pub struct MetricsRegistry {
    inner: Arc<RwLock<BTreeMap<String, MetricDefinition>>>,
}

impl MetricsRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a metric definition.
    ///
    /// Returns `Ok(true)` when the metric is new, `Ok(false)` when the exact
    /// same definition was already present, and an error if the name is reused
    /// with a conflicting definition.
    pub fn register(
        &self,
        definition: MetricDefinition,
    ) -> Result<bool, MetricsRegistryError> {
        let mut definitions = self.inner.write().expect("metrics registry poisoned");
        match definitions.get(&definition.name) {
            Some(existing) if existing == &definition => Ok(false),
            Some(_) => Err(MetricsRegistryError::ConflictingDefinition {
                name: definition.name,
            }),
            None => {
                definitions.insert(definition.name.clone(), definition);
                Ok(true)
            }
        }
    }

    /// Registers multiple definitions, stopping on the first conflict.
    pub fn register_all<I>(&self, definitions: I) -> Result<(), MetricsRegistryError>
    where
        I: IntoIterator<Item = MetricDefinition>,
    {
        for definition in definitions {
            self.register(definition)?;
        }
        Ok(())
    }

    /// Returns all metric definitions in stable name order.
    pub fn definitions(&self) -> Vec<MetricDefinition> {
        self.inner
            .read()
            .expect("metrics registry poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Returns a definition by name when it exists.
    pub fn get(&self, name: &str) -> Option<MetricDefinition> {
        self.inner
            .read()
            .expect("metrics registry poisoned")
            .get(name)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_metric_definitions_in_stable_order() {
        let registry = MetricsRegistry::new();
        registry
            .register(MetricDefinition::new(
                "queue_depth",
                MetricKind::Gauge,
                "Current queue depth",
            ))
            .unwrap();
        registry
            .register(
                MetricDefinition::new(
                    "query_duration_seconds",
                    MetricKind::Histogram,
                    "Observed query latency",
                )
                .with_unit("seconds")
                .with_labels(["path"]),
            )
            .unwrap();

        let definitions = registry.definitions();
        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].name, "query_duration_seconds");
        assert_eq!(definitions[1].name, "queue_depth");
        assert_eq!(definitions[0].labels, vec!["path".to_string()]);
        assert_eq!(definitions[0].unit.as_deref(), Some("seconds"));
    }

    #[test]
    fn duplicate_registration_of_same_definition_is_a_noop() {
        let registry = MetricsRegistry::new();
        let definition = MetricDefinition::new(
            "ingestion_queue_size",
            MetricKind::Gauge,
            "Current ingestion queue depth",
        );

        assert!(registry.register(definition.clone()).unwrap());
        assert!(!registry.register(definition).unwrap());
        assert_eq!(registry.definitions().len(), 1);
    }

    #[test]
    fn conflicting_duplicate_registration_is_rejected() {
        let registry = MetricsRegistry::new();
        registry
            .register(MetricDefinition::new(
                "search_requests_total",
                MetricKind::Counter,
                "Total search requests",
            ))
            .unwrap();

        let err = registry
            .register(MetricDefinition::new(
                "search_requests_total",
                MetricKind::Histogram,
                "Histogram with conflicting type",
            ))
            .unwrap_err();

        assert_eq!(
            err,
            MetricsRegistryError::ConflictingDefinition {
                name: "search_requests_total".to_string(),
            }
        );
    }
}
