use crate::DocumentId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a geographic point with latitude and longitude.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

/// Represents a geographic bounding box.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeoBoundingBox {
    pub top_left: GeoPoint,
    pub bottom_right: GeoPoint,
}

/// Represents a dynamic value in a document payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PayloadValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Timestamp(DateTime<Utc>),
    GeoPoint(GeoPoint),
    Array(Vec<PayloadValue>),
    Object(HashMap<String, PayloadValue>),
}

impl PartialOrd for PayloadValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (PayloadValue::I64(a), PayloadValue::I64(b)) => a.partial_cmp(b),
            (PayloadValue::F64(a), PayloadValue::F64(b)) => a.partial_cmp(b),
            (PayloadValue::String(a), PayloadValue::String(b)) => a.partial_cmp(b),
            (PayloadValue::Timestamp(a), PayloadValue::Timestamp(b)) => a.partial_cmp(b),
            (PayloadValue::I64(a), PayloadValue::F64(b)) => (*a as f64).partial_cmp(b),
            (PayloadValue::F64(a), PayloadValue::I64(b)) => a.partial_cmp(&(*b as f64)),
            (PayloadValue::Bool(a), PayloadValue::Bool(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl PayloadValue {
    pub fn as_object(&self) -> Option<&HashMap<String, PayloadValue>> {
        match self {
            PayloadValue::Object(map) => Some(map),
            _ => None,
        }
    }
}

/// Defines a filter condition for search queries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Filter {
    And {
        filters: Vec<Filter>,
    },
    Or {
        filters: Vec<Filter>,
    },
    Not {
        filter: Box<Filter>,
    },
    Eq {
        field: String,
        value: PayloadValue,
    },
    Ne {
        field: String,
        value: PayloadValue,
    },
    Gt {
        field: String,
        value: PayloadValue,
    },
    Gte {
        field: String,
        value: PayloadValue,
    },
    Lt {
        field: String,
        value: PayloadValue,
    },
    Lte {
        field: String,
        value: PayloadValue,
    },
    In {
        field: String,
        values: Vec<PayloadValue>,
    },
    GeoWithin {
        field: String,
        bounding_box: GeoBoundingBox,
    },
    Exists {
        field: String,
    },
}

impl Filter {
    pub fn matches(&self, payload: &PayloadValue) -> bool {
        match self {
            Filter::And { filters } => filters.iter().all(|f| f.matches(payload)),
            Filter::Or { filters } => filters.iter().any(|f| f.matches(payload)),
            Filter::Not { filter } => !filter.matches(payload),
            Filter::Eq { field, value } => Self::get_value(payload, field).map_or(false, |v| v == value),
            Filter::Ne { field, value } => Self::get_value(payload, field).map_or(true, |v| v != value),
            Filter::Gt { field, value } => Self::compare(payload, field, value, |a, b| a > b),
            Filter::Gte { field, value } => Self::compare(payload, field, value, |a, b| a >= b),
            Filter::Lt { field, value } => Self::compare(payload, field, value, |a, b| a < b),
            Filter::Lte { field, value } => Self::compare(payload, field, value, |a, b| a <= b),
            Filter::In { field, values } => Self::get_value(payload, field).map_or(false, |v| values.contains(v)),
            Filter::Exists { field } => Self::get_value(payload, field).is_some(),
            // GeoWithin is simplified for now (requires GeoPoint implementation)
            Filter::GeoWithin { field, bounding_box } => {
                if let Some(PayloadValue::GeoPoint(p)) = Self::get_value(payload, field) {
                    p.lat <= bounding_box.top_left.lat && 
                    p.lat >= bounding_box.bottom_right.lat &&
                    p.lon >= bounding_box.top_left.lon &&
                    p.lon <= bounding_box.bottom_right.lon
                } else {
                    false
                }
            }
        }
    }

    fn get_value<'a>(payload: &'a PayloadValue, field: &str) -> Option<&'a PayloadValue> {
        // Handle dot notation
        let mut current = payload;
        for part in field.split('.') {
             if let Some(map) = current.as_object() {
                 current = map.get(part)?;
             } else {
                 return None;
             }
        }
        Some(current)
    }

    fn compare<F>(payload: &PayloadValue, field: &str, target: &PayloadValue, op: F) -> bool 
    where F: Fn(&PayloadValue, &PayloadValue) -> bool {
        Self::get_value(payload, field).map_or(false, |v| {
            // Only compare compatible types?
            // PayloadValue definition: PartialOrd is not derived.
            // We implement simple comparison logic.
            // Ideally use OrderedValue for ranges but here we do raw check.
            op(v, target)
        })
    }
}

