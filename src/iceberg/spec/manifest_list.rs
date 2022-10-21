use crate::iceberg::spec::manifest_list_avro_schema::{
    MANIFEST_LIST_V1_SCHEMA, MANIFEST_LIST_V2_SCHEMA,
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
// TODO: Deserialization should really be done based on field-ids and not names (like any other iceberg file)
// Manifest V2 reader should be able to read V1 as well. See https://iceberg.apache.org/spec/#specification
// This is achieved by using default values for fields that are either not present in V1 or
// are optional in V1 but required in V2. Note that this is different from making those fields
// optional in V2.
pub struct ManifestListV2 {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,

    // Not defined in V1. Default to type 0 (data) for V1 manifests
    #[serde(default = "FileType::data")]
    pub content: FileType,

    // Not defined in V1. Default to 0 for V1 manifests
    #[serde(default)]
    pub sequence_number: i64,

    // Not defined in V1. Default to 0 for V1 manifests
    #[serde(default)]
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,

    // Spark writes it with this alias for some reason
    // Optional in V1, default to 0 if not present
    #[serde(alias = "added_data_files_count", default)]
    pub added_files_count: i32,

    // Spark writes it with this alias for some reason
    // Optional in V1, default to 0 if not present
    #[serde(alias = "existing_data_files_count", default)]
    pub existing_files_count: i32,

    // Spark writes it with this alias for some reason
    // Optional in V1, default to 0 if not present
    #[serde(alias = "deleted_data_files_count", default)]
    pub deleted_files_count: i32,

    // Optional in V1, default to 0 if not present
    #[serde(default)]
    pub added_rows_count: i64,

    // Optional in V1, default to 0 if not present
    #[serde(default)]
    pub existing_rows_count: i64,

    // Optional in V1, default to 0 if not present
    #[serde(default)]
    pub deleted_rows_count: i64,

    #[serde(default)]
    pub partitions: Option<Vec<FieldSummaryV2>>,

    #[serde(with = "serde_bytes", default)]
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
// TODO: Deserialization should really be done based on field-ids and not names (like any other iceberg file)
// TODO: Do we really need this V1 struct as according to the iceberg spec V2 readers should read V1 as well? We might if we write to V1
pub struct ManifestListV1 {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub added_snapshot_id: i64,

    // Spark writes it with this alias for some reason.
    // Optional in V1, default to 0 if not present
    #[serde(alias = "added_data_files_count", default)]
    pub added_files_count: Option<i32>,

    // Spark writes it with this alias for some reason
    // Optional in V1, default to 0 if not present
    #[serde(alias = "existing_data_files_count", default)]
    pub existing_files_count: Option<i32>,

    // Spark writes it with this alias for some reason
    // Optional in V1, default to 0 if not present
    #[serde(alias = "deleted_data_files_count", default)]
    pub deleted_files_count: Option<i32>,

    #[serde(default)]
    pub added_rows_count: Option<i64>,
    #[serde(default)]
    pub existing_rows_count: Option<i64>,
    #[serde(default)]
    pub deleted_rows_count: Option<i64>,
    #[serde(default)]
    pub partitions: Option<Vec<FieldSummaryV1>>,
    #[serde(with = "serde_bytes", default)]
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Serialize_repr, Deserialize_repr, Debug, Clone, Eq, PartialEq)]
#[repr(i32)]
pub enum FileType {
    Data = 0,
    Delete = 1,
}

pub type FieldSummaryV1 = FieldSummaryV2;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct FieldSummaryV2 {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    #[serde(with = "serde_bytes")]
    pub lower_bound: Option<Vec<u8>>,
    #[serde(with = "serde_bytes")]
    pub upper_bound: Option<Vec<u8>>,
}

impl ManifestListV2 {
    pub fn raw_avro_schema() -> &'static str {
        MANIFEST_LIST_V2_SCHEMA
    }

    pub fn avro_schema<'a>() -> &'a apache_avro::Schema {
        lazy_static! {
            static ref SCHEMA: apache_avro::Schema =
                apache_avro::Schema::parse_str(MANIFEST_LIST_V2_SCHEMA).unwrap();
        };
        &SCHEMA
    }
}

impl ManifestListV1 {
    pub fn raw_avro_schema() -> &'static str {
        MANIFEST_LIST_V1_SCHEMA
    }

    pub fn avro_schema<'a>() -> &'a apache_avro::Schema {
        lazy_static! {
            static ref SCHEMA: apache_avro::Schema =
                apache_avro::Schema::parse_str(MANIFEST_LIST_V1_SCHEMA).unwrap();
        };
        &SCHEMA
    }
}

impl FileType {
    fn data() -> Self {
        FileType::Data
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    struct Setup {
        resources: PathBuf,
    }

    impl Setup {
        fn new() -> Self {
            let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            d.push("resources/test");
            Setup { resources: d }
        }

        fn manifest_v1(self) -> Vec<u8> {
            let mut v1_file_path = self.resources.clone();
            v1_file_path.push("manifest_list_v1.avro");
            std::fs::read(v1_file_path).unwrap()
        }

        fn manifest_v2(self) -> Vec<u8> {
            let mut v2_file_path = self.resources.clone();
            v2_file_path.push("manifest_list_v2.avro");
            std::fs::read(v2_file_path).unwrap()
        }
    }

    #[test]
    fn test_reading_v1_manifest_file_into_v2() {
        let v1_contents = Setup::new().manifest_v1();
        let reader = apache_avro::Reader::new(v1_contents.as_slice()).unwrap();
        for record in reader {
            let result: ManifestListV2 = apache_avro::from_value(&record.unwrap()).unwrap();
            assert_eq!(
                ManifestListV2 {
                    manifest_path: "file:/Users/jsiva/sw/code/rust/rustberg/test_warehouse/db1.db/db1v1table1/metadata/a3f00225-0cde-48c0-baab-b11dd79d821b-m0.avro".to_string(),
                    manifest_length: 7827,
                    partition_spec_id: 0,
                    content: FileType::Data,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: 9164160847201777787,
                    added_files_count: 2,
                    existing_files_count: 0,
                    deleted_files_count: 0,
                    added_rows_count: 2,
                    existing_rows_count: 0,
                    deleted_rows_count: 0,
                    partitions: Some(
                        vec![
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![10, 0, 0, 0]), upper_bound: Some(vec![12, 0, 0, 0]) },
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![81, 75, 0, 0]), upper_bound: Some(vec![81, 75, 0, 0]) },
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 115, 116, 114, 105, 110, 103]), upper_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 110, 111, 116, 104, 101, 114, 32, 115, 116, 114, 105, 110, 103]) },
                        ]),
                    key_metadata: None,
                },
                result
            )
        }
    }

    #[test]
    fn test_reading_v2_manifest_file_into_v2() {
        let v2_contents = Setup::new().manifest_v2();
        let reader = apache_avro::Reader::new(v2_contents.as_slice()).unwrap();
        for record in reader {
            let result: ManifestListV2 = apache_avro::from_value(&record.unwrap()).unwrap();
            assert_eq!(
                ManifestListV2 {
                    manifest_path: "file:/Users/jsiva/sw/code/rust/rustberg/test_warehouse/db1.db/db1v2table1/metadata/3e48831e-8e8e-418e-92ed-1e01e655dae2-m0.avro".to_string(),
                    manifest_length: 8557,
                    partition_spec_id: 0,
                    content: FileType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 1644494390386601185,
                    added_files_count: 2,
                    existing_files_count: 0,
                    deleted_files_count: 0,
                    added_rows_count: 2,
                    existing_rows_count: 0,
                    deleted_rows_count: 0,
                    partitions: Some(
                        vec![
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![10, 0, 0, 0]), upper_bound: Some(vec![12, 0, 0, 0]) },
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![81, 75, 0, 0]), upper_bound: Some(vec![81, 75, 0, 0]) },
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 115, 116, 114, 105, 110, 103]), upper_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 110, 111, 116, 104, 101, 114, 32, 115, 116, 114, 105, 110, 103]) }]
                    ),
                    key_metadata: None,
                },
                result
            )
        }
    }

    #[test]
    fn test_reading_v1_manifest_file_into_v1() {
        let v1_contents = Setup::new().manifest_v1();
        let reader = apache_avro::Reader::new(v1_contents.as_slice()).unwrap();
        for record in reader {
            let result: ManifestListV1 = apache_avro::from_value(&record.unwrap()).unwrap();
            assert_eq!(
                ManifestListV1 {
                    manifest_path: "file:/Users/jsiva/sw/code/rust/rustberg/test_warehouse/db1.db/db1v1table1/metadata/a3f00225-0cde-48c0-baab-b11dd79d821b-m0.avro".to_string(),
                    manifest_length: 7827,
                    partition_spec_id: 0,
                    added_snapshot_id: 9164160847201777787,
                    added_files_count: Some(2),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(2),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![10, 0, 0, 0]), upper_bound: Some(vec![12, 0, 0, 0]) },
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![81, 75, 0, 0]), upper_bound: Some(vec![81, 75, 0, 0]) },
                            FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 115, 116, 114, 105, 110, 103]), upper_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 110, 111, 116, 104, 101, 114, 32, 115, 116, 114, 105, 110, 103]) },
                        ]),
                    key_metadata: None,
                },
                result
            )
        }
    }

    #[test]
    fn test_manifest_list_v2_roundtrip() {
        let v2_manifest_list =
            ManifestListV2 {
                manifest_path: "file:/Users/jsiva/sw/code/rust/rustberg/test_warehouse/db1.db/db1v2table1/metadata/3e48831e-8e8e-418e-92ed-1e01e655dae2-m0.avro".to_string(),
                manifest_length: 8557,
                partition_spec_id: 0,
                content: FileType::Data,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 1644494390386601185,
                added_files_count: 2,
                existing_files_count: 0,
                deleted_files_count: 0,
                added_rows_count: 2,
                existing_rows_count: 0,
                deleted_rows_count: 0,
                partitions: Some(
                    vec![
                        FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![10, 0, 0, 0]), upper_bound: Some(vec![12, 0, 0, 0]) },
                        FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![81, 75, 0, 0]), upper_bound: Some(vec![81, 75, 0, 0]) },
                        FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 115, 116, 114, 105, 110, 103]), upper_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 110, 111, 116, 104, 101, 114, 32, 115, 116, 114, 105, 110, 103]) }]
                ),
                key_metadata: None,
            };

        let mut writer = apache_avro::Writer::new(ManifestListV2::avro_schema(), Vec::new());
        writer.append_ser(v2_manifest_list.clone()).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = apache_avro::Reader::new(encoded.as_slice()).unwrap();
        for record in reader {
            let result: ManifestListV2 = apache_avro::from_value(&record.unwrap()).unwrap();
            assert_eq!(v2_manifest_list, result);
        }
    }

    #[test]
    fn test_manifest_list_v1_roundtrip() {
        let v1_manifest_list =
            ManifestListV1 {
                manifest_path: "file:/Users/jsiva/sw/code/rust/rustberg/test_warehouse/db1.db/db1v1table1/metadata/a3f00225-0cde-48c0-baab-b11dd79d821b-m0.avro".to_string(),
                manifest_length: 7827,
                partition_spec_id: 0,
                added_snapshot_id: 9164160847201777787,
                added_files_count: Some(2),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(2),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![
                        FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![10, 0, 0, 0]), upper_bound: Some(vec![12, 0, 0, 0]) },
                        FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![81, 75, 0, 0]), upper_bound: Some(vec![81, 75, 0, 0]) },
                        FieldSummaryV2 { contains_null: false, contains_nan: Some(false), lower_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 115, 116, 114, 105, 110, 103]), upper_bound: Some(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 110, 111, 116, 104, 101, 114, 32, 115, 116, 114, 105, 110, 103]) },
                    ]),
                key_metadata: None,
            };

        let mut writer = apache_avro::Writer::new(ManifestListV1::avro_schema(), Vec::new());
        writer.append_ser(v1_manifest_list.clone()).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = apache_avro::Reader::new(encoded.as_slice()).unwrap();
        for record in reader {
            let result: ManifestListV1 = apache_avro::from_value(&record.unwrap()).unwrap();
            assert_eq!(v1_manifest_list, result);
        }
    }
}
