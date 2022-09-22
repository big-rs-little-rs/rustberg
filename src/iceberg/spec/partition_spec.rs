use lazy_static::lazy_static;
use regex::Regex;
use serde::de::{self, IntoDeserializer};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: Transform,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
// Set remote to Self to make it easy to override Serialize and Deserialize implementations
// for specific enum variants such as Bucket and Truncate. This avoid boilerplate for using
// default implementations for others
#[serde(rename_all = "kebab-case", remote = "Self")]
pub enum Transform {
    Identity,
    Bucket(u32),
    Truncate(u32),
    Year,
    Month,
    Day,
    Hour,
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.starts_with("bucket") {
            try_deserialize_bucket(value.into_deserializer())
        } else if value.starts_with("truncate") {
            try_deserialize_truncate(value.into_deserializer())
        } else {
            Self::deserialize(value.into_deserializer())
        }
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Transform::Bucket(bucket) => serializer.serialize_str(&format!("bucket[{}]", bucket)),
            Transform::Truncate(bucket) => {
                serializer.serialize_str(&format!("truncate[{}]", bucket))
            }
            _ => Self::serialize(&self, serializer),
        }
    }
}

fn try_deserialize_bucket<'de, D>(deserializer: D) -> Result<Transform, D::Error>
where
    D: serde::Deserializer<'de>,
{
    lazy_static! {
        static ref REGEX: Regex = Regex::new(r"^bucket\[(?P<bucket>\d+)\]$").unwrap();
    };

    let value = String::deserialize(deserializer)?;

    REGEX
        .captures(&value)
        .ok_or_else(|| de::Error::custom(format!("Wrong bucket format: {}", value)))
        .and_then(|captures| {
            captures
                .name("bucket")
                .ok_or_else(|| de::Error::custom(format!("Wrong bucket format: {}", value)))
        })
        .and_then(|regex_match| {
            regex_match
                .as_str()
                .parse::<u32>()
                .map_err(|_| de::Error::custom(format!("Invalid bucket number: {}", value)))
        })
        .and_then(|num| Ok(Transform::Bucket(num)))
}

fn try_deserialize_truncate<'de, D>(deserializer: D) -> Result<Transform, D::Error>
where
    D: serde::Deserializer<'de>,
{
    lazy_static! {
        static ref REGEX: Regex = Regex::new(r"^truncate\[(?P<truncate>\d+)\]$").unwrap();
    };

    let value = String::deserialize(deserializer)?;

    REGEX
        .captures(&value)
        .ok_or_else(|| de::Error::custom(format!("Wrong truncate format: {}", value)))
        .and_then(|captures| {
            captures
                .name("truncate")
                .ok_or_else(|| de::Error::custom(format!("Wrong truncate format: {}", value)))
        })
        .and_then(|regex_match| {
            regex_match
                .as_str()
                .parse::<u32>()
                .map_err(|_| de::Error::custom(format!("Invalid truncate number: {}", value)))
        })
        .and_then(|num| Ok(Transform::Truncate(num)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_transform_deserialize_fails_on_incorrect_format() {
        let data = r#""bucket(1)""#;
        let transform: Result<Transform, _> = serde_json::from_str(data);

        assert!(transform.is_err())
    }

    #[test]
    fn test_bucket_transform_deserialize_fails_on_incorrect_bucket_number() {
        let data = r#""bucket[a1]""#;
        let transform: Result<Transform, _> = serde_json::from_str(data);

        assert!(transform.is_err())
    }

    #[test]
    fn test_bucket_transform_deserialize() {
        let data = r#""bucket[42]""#;
        let transform = serde_json::from_str(data).unwrap();

        assert_eq!(Transform::Bucket(42), transform);
    }

    #[test]
    fn test_truncate_transform_deserialize_fails_on_incorrect_format() {
        let data = r#""truncate(1)""#;
        let transform: Result<Transform, _> = serde_json::from_str(data);

        assert!(transform.is_err())
    }

    #[test]
    fn test_truncate_transform_deserialize_fails_on_incorrect_bucket_number() {
        let data = r#""truncate[a1]""#;
        let transform: Result<Transform, _> = serde_json::from_str(data);

        assert!(transform.is_err())
    }

    #[test]
    fn test_truncate_transform_deserialize() {
        let data = r#""truncate[42]""#;
        let transform = serde_json::from_str(data).unwrap();

        assert_eq!(Transform::Truncate(42), transform);
    }

    #[test]
    fn test_other_transform_enum_variants() {
        let variants = [
            r#""identity""#,
            r#""year""#,
            r#""month""#,
            r#""day""#,
            r#""hour""#,
        ];
        let transforms = variants.map(|variant| {
            serde_json::from_str::<Transform>(variant)
                .expect(&format!("Failed for variant: {}", variant))
        });
        assert_eq!(
            [
                Transform::Identity,
                Transform::Year,
                Transform::Month,
                Transform::Day,
                Transform::Hour
            ],
            transforms
        )
    }

    #[test]
    fn test_transform_serde_roundtrip() {
        let transforms = [
            Transform::Identity,
            Transform::Year,
            Transform::Month,
            Transform::Day,
            Transform::Hour,
            Transform::Bucket(32),
            Transform::Truncate(42),
        ];

        for transform in transforms {
            let ser = serde_json::to_string(&transform)
                .expect(&format!("Serialization failed for {:?}", &transform));
            let rt_transform = serde_json::from_str::<Transform>(&ser).expect(&format!(
                "Deserializion of serialized transform {:?}, {} failed",
                &transform, ser
            ));
            assert_eq!(transform, rt_transform);
        }
    }

    #[test]
    fn test_partition_spec_deserialize() {
        let partition_spec_json_str = r#"
        {
            "spec-id": 0,
            "fields": [
                {
                    "source-id": 4,
                    "field-id": 1000,
                    "name": "ts_day",
                    "transform": "day"
                },
                {
                    "source-id": 1,
                    "field-id": 1001,
                    "name": "id_bucket",
                    "transform": "bucket[16]"
                }
            ]
        }
        "#;

        let deserialized_partition_spec: PartitionSpec =
            serde_json::from_str(partition_spec_json_str).unwrap();
        let expected_partition_spec = PartitionSpec {
            spec_id: 0,
            fields: vec![
                PartitionField {
                    source_id: 4,
                    field_id: 1000,
                    name: "ts_day".to_string(),
                    transform: Transform::Day,
                },
                PartitionField {
                    source_id: 1,
                    field_id: 1001,
                    name: "id_bucket".to_string(),
                    transform: Transform::Bucket(16),
                },
            ],
        };

        assert_eq!(deserialized_partition_spec, expected_partition_spec);
    }

    #[test]
    fn test_partition_spec_serde_roundtrip() {
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![
                PartitionField {
                    source_id: 4,
                    field_id: 1000,
                    name: "ts_day".to_string(),
                    transform: Transform::Day,
                },
                PartitionField {
                    source_id: 1,
                    field_id: 1001,
                    name: "id_truncate".to_string(),
                    transform: Transform::Truncate(16),
                },
            ],
        };

        let serialized = serde_json::to_string(&spec).unwrap();
        let deserialized: PartitionSpec = serde_json::from_str(&serialized).unwrap();
        assert_eq!(spec, deserialized);
    }
}
