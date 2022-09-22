use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotV2 {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub summary: Summary,
    pub manifest_list: String,
    pub schema_id: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", remote = "Self")]
pub struct SnapshotV1 {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub timestamp_ms: i64,
    pub manifest_list: Option<String>,
    pub manifests: Option<Vec<String>>,
    pub summary: Option<Summary>,
    pub schema_id: Option<i64>,
}

impl<'de> Deserialize<'de> for SnapshotV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let snapshot = Self::deserialize(deserializer)?;
        if snapshot.manifest_list.is_some() && snapshot.manifests.is_some() {
            Err(serde::de::Error::custom(
                "Both manifests and manifest_lists can't be present in snapshot",
            ))
        } else {
            Ok(snapshot)
        }
    }
}

impl Serialize for SnapshotV1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Self::serialize(&self, serializer)
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub struct Summary {
    pub operation: Operation,
    #[serde(flatten)]
    pub rest: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Append,
    Replace,
    Overwrite,
    Delete,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotRefV2 {
    pub snapshot_id: i64,
    #[serde(flatten)]
    pub ref_type: RefType,
    pub max_ref_age_ms: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum RefType {
    #[serde(rename_all = "kebab-case")]
    Branch {
        min_snapshots_to_keep: Option<i32>,
        max_snapshot_age_ms: Option<i64>,
    },
    Tag,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_v2() {
        let data = r#"
        {
          "snapshot-id": 3051729675574597004,
          "parent-snapshot-id": 651729675574597004,
          "sequence-number": 33,
          "timestamp-ms": 1515100955770,
          "summary": {
            "operation": "append"
          },
          "manifest-list": "s3://b/wh/.../s1.avro",
          "schema-id": 0
        }
        "#;

        let deser: SnapshotV2 = serde_json::from_str(data).unwrap();
        assert_eq!(
            SnapshotV2 {
                snapshot_id: 3051729675574597004,
                parent_snapshot_id: Some(651729675574597004),
                sequence_number: 33,
                timestamp_ms: 1515100955770,
                summary: Summary {
                    operation: Operation::Append,
                    rest: HashMap::new()
                },
                manifest_list: "s3://b/wh/.../s1.avro".to_string(),
                schema_id: Some(0),
            },
            deser
        );
    }

    #[test]
    fn test_snapshot_v1() {
        let data = r#"
        {
          "snapshot-id": 3051729675574597004,
          "parent-snapshot-id": 651729675574597004,
          "timestamp-ms": 1515100955770,
          "manifest-list": "s3://b/wh/.../s1.avro"
        }
        "#;

        let deser: SnapshotV1 = serde_json::from_str(data).unwrap();
        assert_eq!(
            SnapshotV1 {
                snapshot_id: 3051729675574597004,
                parent_snapshot_id: Some(651729675574597004),
                timestamp_ms: 1515100955770,
                summary: None,
                manifests: None,
                manifest_list: Some("s3://b/wh/.../s1.avro".to_string()),
                schema_id: None,
            },
            deser
        );
    }

    #[test]
    fn test_snapshot_tag_ref_v2() {
        let data = r#"
        {
          "snapshot-id": 123456789000,
          "type": "tag",
          "max-ref-age-ms": 10000000
        }
        "#;

        let deser: SnapshotRefV2 = serde_json::from_str(data).unwrap();
        assert_eq!(
            SnapshotRefV2 {
                snapshot_id: 123456789000,
                ref_type: RefType::Tag,
                max_ref_age_ms: Some(10000000)
            },
            deser
        );
    }

    #[test]
    fn test_snapshot_tag_branch_v2() {
        let data = r#"
        {
          "snapshot-id": 123456789000,
          "type": "branch",
          "min-snapshots-to-keep": 2345
        }
        "#;

        let deser: SnapshotRefV2 = serde_json::from_str(data).unwrap();
        assert_eq!(
            SnapshotRefV2 {
                snapshot_id: 123456789000,
                ref_type: RefType::Branch {
                    min_snapshots_to_keep: Some(2345),
                    max_snapshot_age_ms: None
                },
                max_ref_age_ms: None
            },
            deser
        );
    }
}
