use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

use super::partition_spec::{PartitionField, PartitionSpec};
use super::schema::{IcebergSchemaV1, IcebergSchemaV2};
use super::snapshot::{SnapshotRefV2, SnapshotV1, SnapshotV2};
use super::sort_orders::SortOrders;

#[derive(Debug, Eq, PartialEq)]
// Write custom serializer and deserializer for TableMetadata to
// delegate to TableMetadataV2 (and other versions in future). Ideally
// We'd not have to do this and instead can utilize tag and rename attributes
// of serde container attributes. However, since the version is a integer
// we can't do #[serde(rename = 2, tag= "format-version")] type of handling.
// serde doesn't yet support non-string tags
pub enum TableMetadata {
    V1(TableMetadataV1),
    V2(TableMetadataV2),
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
pub struct TableMetadataV2 {
    pub format_version: i32,
    pub table_uuid: Uuid,
    pub location: String,
    pub last_sequence_number: i64,
    pub last_updated_ms: i64,
    pub last_column_id: i32,
    pub schemas: Vec<IcebergSchemaV2>,
    pub current_schema_id: i32,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: i32,
    pub last_partition_id: i32,
    pub properties: Option<HashMap<String, String>>,
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Option<Vec<SnapshotV2>>,
    pub snapshot_log: Option<Vec<SnapshotLog>>,
    pub metadata_log: Option<Vec<MetadataLog>>,
    pub sort_orders: Vec<SortOrders>,
    pub default_sort_order_id: i32,
    pub refs: Option<HashMap<String, SnapshotRefV2>>,
    pub statistics: Option<Statistics>, // Unused: See documentation in Statistics structure
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
pub struct TableMetadataV1 {
    pub format_version: i32,
    pub table_uuid: Option<Uuid>,
    pub location: String,
    pub last_updated_ms: i64,
    pub last_column_id: i32,
    pub schema: IcebergSchemaV1,
    pub schemas: Option<Vec<IcebergSchemaV1>>,
    pub current_schema_id: Option<i32>,
    pub partition_spec: Vec<PartitionField>,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: Option<i32>,
    pub last_partition_id: Option<i32>,
    pub properties: Option<HashMap<String, String>>,
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Option<Vec<SnapshotV1>>,
    pub snapshot_log: Option<Vec<SnapshotLog>>,
    pub metadata_log: Option<Vec<MetadataLog>>,
    pub sort_orders: Option<Vec<SortOrders>>,
    pub default_sort_order_id: i32,
    pub statistics: Option<Statistics>, // Unused: See documentation in Statistics structure
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLog {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLog {
    pub metadata_file: String,
    pub timestamp_ms: i64,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Statistics {
    // We are not going to implement this yet. Statistics must be read from
    // puffin files, but they are optional for readers to read
}

impl<'de> Deserialize<'de> for TableMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let format_version = value.get("format-version").ok_or_else(|| {
            serde::de::Error::custom("Unable to find 'format-version' key in metadata")
        })?;
        let format_version = format_version.as_i64().ok_or_else(|| {
            serde::de::Error::custom(format!(
                "Invalid 'format-version' in metadata: {:?}",
                format_version
            ))
        })?;

        match format_version {
            2 => TableMetadataV2::deserialize(value)
                .map(TableMetadata::V2)
                .map_err(|e| {
                    serde::de::Error::custom(format!(
                        "Unable to deserialize version 2 metadata: error: {}",
                        e
                    ))
                }),
            1 => TableMetadataV1::deserialize(value)
                .map(TableMetadata::V1)
                .map_err(|e| {
                    serde::de::Error::custom(format!(
                        "Unable to deserialize version 1 metadata: error: {}",
                        e
                    ))
                }),
            _ => Err(serde::de::Error::custom(format!(
                "Unsupported metadata format-version {}",
                format_version
            ))),
        }
    }
}

impl Serialize for TableMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Shadow the TableMetadata. This is mainly so that in the match arm below we can take
        // a reference and use references all the way and avoid cloning the metadata
        #[derive(Serialize)]
        #[serde(untagged)]
        enum TableMetadataShadow<'a> {
            V1(&'a TableMetadataV1),
            V2(&'a TableMetadataV2),
        }

        #[derive(Serialize)]
        #[serde(rename_all = "kebab-case")]
        struct VersionedTableMetadata<'a> {
            format_version: i32,
            #[serde(flatten)]
            metadata: TableMetadataShadow<'a>,
        }

        let meta = match self {
            TableMetadata::V2(metadata) => VersionedTableMetadata {
                format_version: 2,
                metadata: TableMetadataShadow::V2(metadata),
            },
            TableMetadata::V1(metadata) => VersionedTableMetadata {
                format_version: 1,
                metadata: TableMetadataShadow::V1(metadata),
            },
        };

        meta.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v1_metadata() {
        let example_v1_metadata = r#"
        {
          "format-version" : 1,
          "table-uuid" : "5ff386a7-6dfc-4519-9a24-99e10c212081",
          "location" : "file:/home/someone/sw/code/rust/rustberg/test_warehouse/db1.db/db1v1table1",
          "last-updated-ms" : 1665194853343,
          "last-column-id" : 12,
          "schema" : {
            "type" : "struct",
            "schema-id" : 0,
            "fields" : [ {
              "id" : 1,
              "name" : "byte0",
              "required" : false,
              "type" : "int",
              "doc" : "byte data type column, rev 0"
            }, {
              "id" : 2,
              "name" : "bool0",
              "required" : false,
              "type" : "boolean",
              "doc" : "boolean data type column, rev 0"
            }, {
              "id" : 3,
              "name" : "short0",
              "required" : false,
              "type" : "int",
              "doc" : "short data type column, rev 0"
            }, {
              "id" : 4,
              "name" : "int0",
              "required" : false,
              "type" : "int",
              "doc" : "integer data type column, rev 0"
            }, {
              "id" : 5,
              "name" : "long0",
              "required" : false,
              "type" : "long",
              "doc" : "long data type column, rev 0"
            }, {
              "id" : 6,
              "name" : "float0",
              "required" : false,
              "type" : "float",
              "doc" : "float data type column, rev 0"
            }, {
              "id" : 7,
              "name" : "double0",
              "required" : false,
              "type" : "double",
              "doc" : "double data type column, rev 0"
            }, {
              "id" : 8,
              "name" : "date0",
              "required" : false,
              "type" : "date",
              "doc" : "date data type column, rev 0"
            }, {
              "id" : 9,
              "name" : "timestamp0",
              "required" : false,
              "type" : "timestamptz",
              "doc" : "timestamp data type column, rev 0"
            }, {
              "id" : 10,
              "name" : "string0",
              "required" : false,
              "type" : "string",
              "doc" : "string data type column, rev 0"
            }, {
              "id" : 11,
              "name" : "binary0",
              "required" : false,
              "type" : "binary",
              "doc" : "binary data type column, rev 0"
            }, {
              "id" : 12,
              "name" : "decimal0",
              "required" : false,
              "type" : "decimal(10, 0)",
              "doc" : "decimal data type column, rev 0"
            } ]
          },
          "current-schema-id" : 0,
          "schemas" : [ {
            "type" : "struct",
            "schema-id" : 0,
            "fields" : [ {
              "id" : 1,
              "name" : "byte0",
              "required" : false,
              "type" : "int",
              "doc" : "byte data type column, rev 0"
            }, {
              "id" : 2,
              "name" : "bool0",
              "required" : false,
              "type" : "boolean",
              "doc" : "boolean data type column, rev 0"
            }, {
              "id" : 3,
              "name" : "short0",
              "required" : false,
              "type" : "int",
              "doc" : "short data type column, rev 0"
            }, {
              "id" : 4,
              "name" : "int0",
              "required" : false,
              "type" : "int",
              "doc" : "integer data type column, rev 0"
            }, {
              "id" : 5,
              "name" : "long0",
              "required" : false,
              "type" : "long",
              "doc" : "long data type column, rev 0"
            }, {
              "id" : 6,
              "name" : "float0",
              "required" : false,
              "type" : "float",
              "doc" : "float data type column, rev 0"
            }, {
              "id" : 7,
              "name" : "double0",
              "required" : false,
              "type" : "double",
              "doc" : "double data type column, rev 0"
            }, {
              "id" : 8,
              "name" : "date0",
              "required" : false,
              "type" : "date",
              "doc" : "date data type column, rev 0"
            }, {
              "id" : 9,
              "name" : "timestamp0",
              "required" : false,
              "type" : "timestamptz",
              "doc" : "timestamp data type column, rev 0"
            }, {
              "id" : 10,
              "name" : "string0",
              "required" : false,
              "type" : "string",
              "doc" : "string data type column, rev 0"
            }, {
              "id" : 11,
              "name" : "binary0",
              "required" : false,
              "type" : "binary",
              "doc" : "binary data type column, rev 0"
            }, {
              "id" : 12,
              "name" : "decimal0",
              "required" : false,
              "type" : "decimal(10, 0)",
              "doc" : "decimal data type column, rev 0"
            } ]
          } ],
          "partition-spec" : [ {
            "name" : "byte0_bucket",
            "transform" : "bucket[16]",
            "source-id" : 1,
            "field-id" : 1000
          }, {
            "name" : "timestamp0_day",
            "transform" : "day",
            "source-id" : 9,
            "field-id" : 1001
          }, {
            "name" : "string0",
            "transform" : "identity",
            "source-id" : 10,
            "field-id" : 1002
          } ],
          "default-spec-id" : 0,
          "partition-specs" : [ {
            "spec-id" : 0,
            "fields" : [ {
              "name" : "byte0_bucket",
              "transform" : "bucket[16]",
              "source-id" : 1,
              "field-id" : 1000
            }, {
              "name" : "timestamp0_day",
              "transform" : "day",
              "source-id" : 9,
              "field-id" : 1001
            }, {
              "name" : "string0",
              "transform" : "identity",
              "source-id" : 10,
              "field-id" : 1002
            } ]
          } ],
          "last-partition-id" : 1002,
          "default-sort-order-id" : 0,
          "sort-orders" : [ {
            "order-id" : 0,
            "fields" : [ ]
          } ],
          "properties" : {
            "owner" : "someone"
          },
          "current-snapshot-id" : 935718495670614874,
          "refs" : {
            "main" : {
              "snapshot-id" : 935718495670614874,
              "type" : "branch"
            }
          },
          "snapshots" : [ {
            "snapshot-id" : 935718495670614874,
            "timestamp-ms" : 1665194853343,
            "summary" : {
              "operation" : "append",
              "spark.app.id" : "local-1665194845087",
              "added-data-files" : "2",
              "added-records" : "2",
              "added-files-size" : "7818",
              "changed-partition-count" : "2",
              "total-records" : "2",
              "total-files-size" : "7818",
              "total-data-files" : "2",
              "total-delete-files" : "0",
              "total-position-deletes" : "0",
              "total-equality-deletes" : "0"
            },
            "manifest-list" : "file:/home/someone/sw/code/rust/rustberg/test_warehouse/db1.db/db1v1table1/metadata/snap-935718495670614874-1-83e34c0e-1bfd-4484-97c9-5a413da77b41.avro",
            "schema-id" : 0
          } ],
          "snapshot-log" : [ {
            "timestamp-ms" : 1665194853343,
            "snapshot-id" : 935718495670614874
          } ],
          "metadata-log" : [ {
            "timestamp-ms" : 1665194848817,
            "metadata-file" : "file:/home/someone/sw/code/rust/rustberg/test_warehouse/db1.db/db1v1table1/metadata/00000-fb84b7f0-e72d-48e0-91e1-f0ee7087f471.metadata.json"
          } ]
        }
        "#;

        let v1_metadata: TableMetadata =
            serde_json::from_str(example_v1_metadata).expect("Unable to deserialize metadata");

        // Test roundtrip
        let v1_metadata_ser =
            serde_json::to_string(&v1_metadata).expect("Serializing v1_metadata failed");
        let v1_metadata_deser = serde_json::from_str(&v1_metadata_ser)
            .expect("Deserializing serialized v1_metadata failed");

        assert_eq!(v1_metadata, v1_metadata_deser);
    }

    #[test]
    fn test_v2_metadata() {
        let example_v2_metadata = r#"
        {
          "format-version" : 2,
          "table-uuid" : "1cbafffd-0066-4eb8-9e09-b69b2f8e0d2a",
          "location" : "file:/home/someone/sw/code/rust/rustberg/test_warehouse/db1.db/db1v2table1",
          "last-sequence-number" : 1,
          "last-updated-ms" : 1665194853904,
          "last-column-id" : 12,
          "current-schema-id" : 0,
          "schemas" : [ {
            "type" : "struct",
            "schema-id" : 0,
            "fields" : [ {
              "id" : 1,
              "name" : "byte0",
              "required" : false,
              "type" : "int",
              "doc" : "byte data type column, rev 0"
            }, {
              "id" : 2,
              "name" : "bool0",
              "required" : false,
              "type" : "boolean",
              "doc" : "boolean data type column, rev 0"
            }, {
              "id" : 3,
              "name" : "short0",
              "required" : false,
              "type" : "int",
              "doc" : "short data type column, rev 0"
            }, {
              "id" : 4,
              "name" : "int0",
              "required" : false,
              "type" : "int",
              "doc" : "integer data type column, rev 0"
            }, {
              "id" : 5,
              "name" : "long0",
              "required" : false,
              "type" : "long",
              "doc" : "long data type column, rev 0"
            }, {
              "id" : 6,
              "name" : "float0",
              "required" : false,
              "type" : "float",
              "doc" : "float data type column, rev 0"
            }, {
              "id" : 7,
              "name" : "double0",
              "required" : false,
              "type" : "double",
              "doc" : "double data type column, rev 0"
            }, {
              "id" : 8,
              "name" : "date0",
              "required" : false,
              "type" : "date",
              "doc" : "date data type column, rev 0"
            }, {
              "id" : 9,
              "name" : "timestamp0",
              "required" : false,
              "type" : "timestamptz",
              "doc" : "timestamp data type column, rev 0"
            }, {
              "id" : 10,
              "name" : "string0",
              "required" : false,
              "type" : "string",
              "doc" : "string data type column, rev 0"
            }, {
              "id" : 11,
              "name" : "binary0",
              "required" : false,
              "type" : "binary",
              "doc" : "binary data type column, rev 0"
            }, {
              "id" : 12,
              "name" : "decimal0",
              "required" : false,
              "type" : "decimal(10, 0)",
              "doc" : "decimal data type column, rev 0"
            } ]
          } ],
          "default-spec-id" : 0,
          "partition-specs" : [ {
            "spec-id" : 0,
            "fields" : [ {
              "name" : "byte0_bucket",
              "transform" : "bucket[16]",
              "source-id" : 1,
              "field-id" : 1000
            }, {
              "name" : "timestamp0_day",
              "transform" : "day",
              "source-id" : 9,
              "field-id" : 1001
            }, {
              "name" : "string0",
              "transform" : "identity",
              "source-id" : 10,
              "field-id" : 1002
            } ]
          } ],
          "last-partition-id" : 1002,
          "default-sort-order-id" : 0,
          "sort-orders" : [ {
            "order-id" : 0,
            "fields" : [ ]
          } ],
          "properties" : {
            "owner" : "someone"
          },
          "current-snapshot-id" : 6627642968708327025,
          "refs" : {
            "main" : {
              "snapshot-id" : 6627642968708327025,
              "type" : "branch"
            }
          },
          "snapshots" : [ {
            "sequence-number" : 1,
            "snapshot-id" : 6627642968708327025,
            "timestamp-ms" : 1665194853904,
            "summary" : {
              "operation" : "append",
              "spark.app.id" : "local-1665194845087",
              "added-data-files" : "2",
              "added-records" : "2",
              "added-files-size" : "7818",
              "changed-partition-count" : "2",
              "total-records" : "2",
              "total-files-size" : "7818",
              "total-data-files" : "2",
              "total-delete-files" : "0",
              "total-position-deletes" : "0",
              "total-equality-deletes" : "0"
            },
            "manifest-list" : "file:/home/someone/sw/code/rust/rustberg/test_warehouse/db1.db/db1v2table1/metadata/snap-6627642968708327025-1-b49a8ca6-305a-47c6-ac6e-09acbd78ba00.avro",
            "schema-id" : 0
          } ],
          "snapshot-log" : [ {
            "timestamp-ms" : 1665194853904,
            "snapshot-id" : 6627642968708327025
          } ],
          "metadata-log" : [ {
            "timestamp-ms" : 1665194850314,
            "metadata-file" : "file:/home/someone/sw/code/rust/rustberg/test_warehouse/db1.db/db1v2table1/metadata/00000-dfb95aa3-93b3-42c4-a228-1acf61ea2305.metadata.json"
          } ]
        }
        "#;

        let v2_metadata: TableMetadata =
            serde_json::from_str(example_v2_metadata).expect("Unable to deserialize metadata");

        // Test roundtrip
        let v2_metadata_ser =
            serde_json::to_string(&v2_metadata).expect("Serializing v2_metadata failed");
        let v2_metadata_deser = serde_json::from_str(&v2_metadata_ser)
            .expect("Deserializing serialized v2_metadata failed");

        assert_eq!(v2_metadata, v2_metadata_deser);
    }
}
