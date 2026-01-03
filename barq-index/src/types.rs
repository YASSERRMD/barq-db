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
