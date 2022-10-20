use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ManifestListV2 {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub content: FileType,
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: i32,
    pub existing_files_count: i32,
    pub deleted_files_count: i32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
    pub partitions: Option<Vec<FieldSummaryV2>>,
    #[serde(with = "serde_bytes")]
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ManifestListV1 {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub added_snapshot_id: i64,
    pub added_files_count: Option<i32>,
    pub existing_files_count: Option<i32>,
    pub deleted_files_count: Option<i32>,
    pub added_rows_count: Option<i64>,
    pub existing_rows_count: Option<i64>,
    pub deleted_rows_count: Option<i64>,
    pub partitions: Option<Vec<FieldSummaryV1>>,
    #[serde(with = "serde_bytes")]
    pub key_metadata: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum FileType {
    DATA = 0,
    DELETE = 1,
}

pub type FieldSummaryV1 = FieldSummaryV2;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct FieldSummaryV2 {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    #[serde(with = "serde_bytes")]
    pub lower_bound: Option<Vec<u8>>,
    #[serde(with = "serde_bytes")]
    pub upper_bound: Option<Vec<u8>>,
}