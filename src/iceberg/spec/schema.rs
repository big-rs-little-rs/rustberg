use once_cell::sync::Lazy;
use regex::Regex;
use serde::de::{self, IntoDeserializer};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergSchemaV2 {
    pub schema_id: i32,
    pub identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    pub schema: StructType,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergSchemaV1 {
    pub schema_id: Option<i32>,
    pub identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    pub schema: StructType,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "type", rename = "struct")]
pub struct StructType {
    pub fields: Vec<StructField>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct StructField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: IcebergType,
    pub doc: Option<String>,
    pub initial_default: Option<String>, // Optional JSON encoded value
    pub write_default: Option<String>,   // Optional JSON encoded value
}

// An enum encompassing all the types representable by Iceberg Schema
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
// Set remote to Self to make it easy to override Serialize and Deserialize implementations
// for specific enum variants such as Fixed and Decimal. This avoid boilerplate for using
// default implementations for others
#[serde(rename_all = "kebab-case", untagged)]
pub enum IcebergType {
    // Untagged type. Wrap all untagged types in BasicType enum to make it easier
    // for Serde to decode IcebergType from JSON. Serde can't yet by itself deal with
    // both tagged and untagged items. Hence this workaround. Another approach to deal
    // with the mixing of untagged and tagged enums in JSON is to make this IcebergType
    // a private enum while having a public IcebergType enum that flattens the variants
    // directly into the public enum. Then, we can essentially use the same approach here
    // for the private enum and get the serializers/deserializers for the public enum by
    // implementing the From trait to convert back and forth from public to private and using
    // the from and into container attributes of serde to get the serializers/deserializers
    // for the public type. This involves additional conversions and boilerplate leading to
    // more chance of errors
    Primitive(PrimitiveType),
    // Tagged types (note: contained types are annotated with tags in serde)
    Struct(StructType),
    List(ListType),
    Map(MapType),
}

// An enum to represent untagged types in Iceberg Schema. Untagged types are represented
// directly by a JSON string, whereas tagged types are represented as JSON objects which
// have the key 'type' and hence are tagged
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
// Set remote to Self to make it easy to override Serialize and Deserialize implementations
// for specific enum variants such as Fixed and Decimal. This avoid boilerplate for using
// default implementations for others
#[serde(rename_all = "lowercase", remote = "Self")]
pub enum PrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal { precision: u8, scale: u32 }, // precision must be 38 or less
    Date,
    Time,
    Timestamp,
    Timestamptz,
    String,
    Uuid,
    Fixed(u32),
    Binary,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "type", rename = "list")]
pub struct ListType {
    pub element_id: i32,
    pub element_required: bool,
    pub element: Box<IcebergType>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "type", rename = "map")]
pub struct MapType {
    pub key_id: i32,
    pub key: Box<IcebergType>,
    pub value_id: i32,
    pub value_required: bool,
    pub value: Box<IcebergType>,
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        // Although strictly not necessary to do the starts_with check, this helps
        // in giving better errors where 'fixed' and 'decimal' types are further denoted
        // improperly
        if value.starts_with("fixed") {
            try_deserialize_fixed_type(value.into_deserializer())
        } else if value.starts_with("decimal") {
            try_deserialize_decimal_type(value.into_deserializer())
        } else {
            Self::deserialize(value.into_deserializer())
        }
    }
}

fn try_deserialize_fixed_type<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    static REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^fixed\[(?P<bkt>\d+)]$").unwrap());
    let value = String::deserialize(deserializer)?;

    REGEX
        .captures(&value)
        .ok_or_else(|| de::Error::custom(format!("Wrong fixed type format: {}", value)))
        .and_then(|captures| {
            captures
                .name("bkt")
                .ok_or_else(|| de::Error::custom(format!("Wrong fixed type format: {}", value)))
        })
        .and_then(|regex_match| {
            regex_match
                .as_str()
                .parse::<u32>()
                .map_err(|_| de::Error::custom(format!("Invalid fixed type length: {}", value)))
        })
        .map(PrimitiveType::Fixed)
}

fn try_deserialize_decimal_type<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    static REGEX: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^decimal\((?P<p>\d+)\s*,\s*(?P<s>\d+)\)$").unwrap());
    let value = String::deserialize(deserializer)?;

    if let Some(captures) = REGEX.captures(&value) {
        // Get precision
        let precision = if let Some(regex_match) = captures.name("p") {
            if let Ok(precision) = regex_match.as_str().parse::<u8>() {
                if precision > 38 {
                    return Err(de::Error::custom(format!(
                        "Wrong decimal precision. Must be < 38: {}",
                        value
                    )));
                } else {
                    precision
                }
            } else {
                return Err(de::Error::custom(format!(
                    "Wrong decimal precision: {}",
                    value
                )));
            }
        } else {
            return Err(de::Error::custom(format!(
                "Wrong decimal type format: {}",
                value
            )));
        };

        // Get scale
        let scale = if let Some(regex_match) = captures.name("s") {
            if let Ok(scale) = regex_match.as_str().parse::<u32>() {
                scale
            } else {
                return Err(de::Error::custom(format!("Wrong decimal scale: {}", value)));
            }
        } else {
            return Err(de::Error::custom(format!(
                "Wrong decimal type format: {}",
                value
            )));
        };

        Ok(PrimitiveType::Decimal { precision, scale })
    } else {
        Err(de::Error::custom(format!(
            "Wrong decimal type format: {}",
            value
        )))
    }
}

impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PrimitiveType::Fixed(length) => serializer.serialize_str(&format!("fixed[{}]", length)),
            PrimitiveType::Decimal { precision, scale } => {
                serializer.serialize_str(&format!("decimal({}, {})", precision, scale))
            }
            _ => Self::serialize(self, serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_type_deser_fails_on_incorrect_format() {
        let data = [r#""fixed(1)"""#, r#""fixed[a]""#];
        for datum in data {
            let iceberg_type = serde_json::from_str::<PrimitiveType>(datum);
            assert!(iceberg_type.is_err())
        }
    }

    #[test]
    fn test_decimal_type_deser_fails_on_incorrect_format() {
        let data = [r#""decimal(1)"""#, r#""decimal[40,2]""#];
        for datum in data {
            let iceberg_type = serde_json::from_str::<PrimitiveType>(datum);
            assert!(iceberg_type.is_err())
        }
    }

    #[test]
    fn test_fixed_type_deser() {
        let data = [r#""fixed[1]""#, r#""fixed[400]""#];
        let iceberg_types = data.map(|datum| {
            serde_json::from_str::<PrimitiveType>(datum)
                .expect(&format!("Failed for variant {}", datum))
        });
        assert_eq!(
            [PrimitiveType::Fixed(1), PrimitiveType::Fixed(400)],
            iceberg_types
        )
    }

    #[test]
    fn test_decimal_type_deser() {
        let data = [r#""decimal(1, 20)""#, r#""decimal(38, 2)""#];
        let iceberg_types = data.map(|datum| {
            serde_json::from_str::<PrimitiveType>(datum)
                .expect(&format!("Failed for variant {}", datum))
        });
        assert_eq!(
            [
                PrimitiveType::Decimal {
                    precision: 1,
                    scale: 20
                },
                PrimitiveType::Decimal {
                    precision: 38,
                    scale: 2
                }
            ],
            iceberg_types
        )
    }

    #[test]
    fn test_fixed_and_decimal_type_serde_roundtrip() {
        let iceberg_types = [
            PrimitiveType::Decimal {
                precision: 1,
                scale: 20,
            },
            PrimitiveType::Decimal {
                precision: 38,
                scale: 2,
            },
            PrimitiveType::Fixed(1),
            PrimitiveType::Fixed(400),
        ];

        for iceberg_type in iceberg_types {
            let ser = serde_json::to_string(&iceberg_type)
                .expect(&format!("Failed to serialize {:?}", iceberg_type));
            let deser: PrimitiveType =
                serde_json::from_str(&ser).expect(&format!("Failed to deser {:?}", ser));
            assert_eq!(iceberg_type, deser);
        }
    }

    #[test]
    fn test_primitive_types_deser() {
        let data = [
            r#""boolean""#,
            r#""int""#,
            r#""long""#,
            r#""float""#,
            r#""double""#,
            r#""date""#,
            r#""time""#,
            r#""timestamp""#,
            r#""timestamptz""#,
            r#""string""#,
            r#""uuid""#,
            r#""binary""#,
        ];

        let iceberg_types = data.map(|datum| {
            serde_json::from_str::<PrimitiveType>(datum)
                .expect(&format!("Failed for variant {}", datum))
        });

        assert_eq!(
            [
                PrimitiveType::Boolean,
                PrimitiveType::Int,
                PrimitiveType::Long,
                PrimitiveType::Float,
                PrimitiveType::Double,
                PrimitiveType::Date,
                PrimitiveType::Time,
                PrimitiveType::Timestamp,
                PrimitiveType::Timestamptz,
                PrimitiveType::String,
                PrimitiveType::Uuid,
                PrimitiveType::Binary
            ],
            iceberg_types
        );
    }

    #[test]
    fn test_primitive_types_serde_roundtrip() {
        let iceberg_types = [
            PrimitiveType::Boolean,
            PrimitiveType::Int,
            PrimitiveType::Long,
            PrimitiveType::Float,
            PrimitiveType::Double,
            PrimitiveType::Date,
            PrimitiveType::Time,
            PrimitiveType::Timestamp,
            PrimitiveType::Timestamptz,
            PrimitiveType::String,
            PrimitiveType::Uuid,
            PrimitiveType::Binary,
        ];

        for iceberg_type in iceberg_types {
            let ser = serde_json::to_string(&iceberg_type)
                .expect(&format!("Failed to serialize {:?}", iceberg_type));
            let deser: PrimitiveType =
                serde_json::from_str(&ser).expect(&format!("Failed to deser {:?}", ser));
            assert_eq!(iceberg_type, deser);
        }
    }

    #[test]
    fn test_struct_deserialize() {
        let data = r#"
        {
          "type": "struct",
          "fields": [ {
            "id": 1,
            "name": "id",
            "required": true,
            "type": "uuid",
            "initial-default": "0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb",
            "write-default": "ec5911be-b0a7-458c-8438-c9a3e53cffae"
          }, {
            "id": 2,
            "name": "list_data",
            "required": false,
            "type": {
              "type": "list",
              "element-id": 3,
              "element-required": true,
              "element": "string" 
            }
          }, {
            "id": 3,
            "name": "map_data",
            "required": true,
            "type": {
              "type": "map",
              "key-id": 4,
              "key": "decimal(30, 20)",
              "value-id": 5,
              "value-required": false,
              "value": "double"
            }
          }, {
            "id": 4,
            "name": "struct_data",
            "required": true,
            "type": {
                  "type": "struct",
                  "fields": [ {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "long"
                  }, {
                    "id": 2,
                    "name": "list_fixed",
                    "required": false,
                    "type": {
                      "type": "list",
                      "element-id": 3,
                      "element-required": true,
                      "element": "fixed[400]" 
                    }
                  }]
            }
          } ]
        }
        "#;

        let deser: IcebergType = serde_json::from_str(data).unwrap();
        assert_eq!(
            IcebergType::Struct(StructType {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        field_type: IcebergType::Primitive(PrimitiveType::Uuid),
                        doc: None,
                        initial_default: Some("0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb".to_string()),
                        write_default: Some("ec5911be-b0a7-458c-8438-c9a3e53cffae".to_string())
                    },
                    StructField {
                        id: 2,
                        name: "list_data".to_string(),
                        required: false,
                        field_type: IcebergType::List(ListType {
                            element_id: 3,
                            element_required: true,
                            element: Box::new(IcebergType::Primitive(PrimitiveType::String))
                        }),
                        doc: None,
                        initial_default: None,
                        write_default: None
                    },
                    StructField {
                        id: 3,
                        name: "map_data".to_string(),
                        required: true,
                        field_type: IcebergType::Map(MapType {
                            key_id: 4,
                            key: Box::new(IcebergType::Primitive(PrimitiveType::Decimal {
                                precision: 30,
                                scale: 20
                            })),
                            value_id: 5,
                            value_required: false,
                            value: Box::new(IcebergType::Primitive(PrimitiveType::Double))
                        }),
                        doc: None,
                        initial_default: None,
                        write_default: None
                    },
                    StructField {
                        id: 4,
                        name: "struct_data".to_string(),
                        required: true,
                        field_type: IcebergType::Struct(StructType {
                            fields: vec![
                                StructField {
                                    id: 1,
                                    name: "id".to_string(),
                                    required: true,
                                    field_type: IcebergType::Primitive(PrimitiveType::Long),
                                    doc: None,
                                    initial_default: None,
                                    write_default: None
                                },
                                StructField {
                                    id: 2,
                                    name: "list_fixed".to_string(),
                                    required: false,
                                    field_type: IcebergType::List(ListType {
                                        element_id: 3,
                                        element_required: true,
                                        element: Box::new(IcebergType::Primitive(
                                            PrimitiveType::Fixed(400)
                                        ))
                                    }),
                                    doc: None,
                                    initial_default: None,
                                    write_default: None
                                },
                            ]
                        }),
                        doc: None,
                        initial_default: None,
                        write_default: None
                    },
                ]
            }),
            deser
        );
    }

    #[test]
    fn test_iceberg_struct_serde_roundtrip() {
        let ib_struct = IcebergType::Struct(StructType {
            fields: vec![
                StructField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive(PrimitiveType::Uuid),
                    doc: None,
                    initial_default: Some("0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb".to_string()),
                    write_default: Some("ec5911be-b0a7-458c-8438-c9a3e53cffae".to_string()),
                },
                StructField {
                    id: 2,
                    name: "list_data".to_string(),
                    required: false,
                    field_type: IcebergType::List(ListType {
                        element_id: 3,
                        element_required: true,
                        element: Box::new(IcebergType::Primitive(PrimitiveType::String)),
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 3,
                    name: "map_data".to_string(),
                    required: true,
                    field_type: IcebergType::Map(MapType {
                        key_id: 4,
                        key: Box::new(IcebergType::Primitive(PrimitiveType::Decimal {
                            precision: 30,
                            scale: 20,
                        })),
                        value_id: 5,
                        value_required: false,
                        value: Box::new(IcebergType::Primitive(PrimitiveType::Double)),
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 4,
                    name: "struct_data".to_string(),
                    required: true,
                    field_type: IcebergType::Struct(StructType {
                        fields: vec![
                            StructField {
                                id: 1,
                                name: "id".to_string(),
                                required: true,
                                field_type: IcebergType::Primitive(PrimitiveType::Long),
                                doc: None,
                                initial_default: None,
                                write_default: None,
                            },
                            StructField {
                                id: 2,
                                name: "list_fixed".to_string(),
                                required: false,
                                field_type: IcebergType::List(ListType {
                                    element_id: 3,
                                    element_required: true,
                                    element: Box::new(IcebergType::Primitive(
                                        PrimitiveType::Fixed(400),
                                    )),
                                }),
                                doc: None,
                                initial_default: None,
                                write_default: None,
                            },
                        ],
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
        });

        let ser = serde_json::to_string(&ib_struct).unwrap();
        let deser: IcebergType = serde_json::from_str(&ser).unwrap();
        assert_eq!(ib_struct, deser);
    }

    #[test]
    fn test_iceberg_schema_v2() {
        let data = r#"
        {
          "schema-id": 123,
          "identifier-field-ids": [1, 2, 3],
          "type": "struct",
          "fields": [ {
            "id": 1,
            "name": "id",
            "required": true,
            "type": "uuid",
            "initial-default": "0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb",
            "write-default": "ec5911be-b0a7-458c-8438-c9a3e53cffae"
          }, {
            "id": 2,
            "name": "list_data",
            "required": false,
            "type": {
              "type": "list",
              "element-id": 3,
              "element-required": true,
              "element": "string" 
            }
          }, {
            "id": 3,
            "name": "map_data",
            "required": true,
            "type": {
              "type": "map",
              "key-id": 4,
              "key": "decimal(30,20)",
              "value-id": 5,
              "value-required": false,
              "value": "double"
            }
          }, {
            "id": 4,
            "name": "struct_data",
            "required": true,
            "type": {
                  "type": "struct",
                  "fields": [ {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "long"
                  }, {
                    "id": 2,
                    "name": "list_fixed",
                    "required": false,
                    "type": {
                      "type": "list",
                      "element-id": 3,
                      "element-required": true,
                      "element": "fixed[400]" 
                    }
                  }]
            }
          } ]
        }
        "#;

        let ib_struct = StructType {
            fields: vec![
                StructField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive(PrimitiveType::Uuid),
                    doc: None,
                    initial_default: Some("0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb".to_string()),
                    write_default: Some("ec5911be-b0a7-458c-8438-c9a3e53cffae".to_string()),
                },
                StructField {
                    id: 2,
                    name: "list_data".to_string(),
                    required: false,
                    field_type: IcebergType::List(ListType {
                        element_id: 3,
                        element_required: true,
                        element: Box::new(IcebergType::Primitive(PrimitiveType::String)),
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 3,
                    name: "map_data".to_string(),
                    required: true,
                    field_type: IcebergType::Map(MapType {
                        key_id: 4,
                        key: Box::new(IcebergType::Primitive(PrimitiveType::Decimal {
                            precision: 30,
                            scale: 20,
                        })),
                        value_id: 5,
                        value_required: false,
                        value: Box::new(IcebergType::Primitive(PrimitiveType::Double)),
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 4,
                    name: "struct_data".to_string(),
                    required: true,
                    field_type: IcebergType::Struct(StructType {
                        fields: vec![
                            StructField {
                                id: 1,
                                name: "id".to_string(),
                                required: true,
                                field_type: IcebergType::Primitive(PrimitiveType::Long),
                                doc: None,
                                initial_default: None,
                                write_default: None,
                            },
                            StructField {
                                id: 2,
                                name: "list_fixed".to_string(),
                                required: false,
                                field_type: IcebergType::List(ListType {
                                    element_id: 3,
                                    element_required: true,
                                    element: Box::new(IcebergType::Primitive(
                                        PrimitiveType::Fixed(400),
                                    )),
                                }),
                                doc: None,
                                initial_default: None,
                                write_default: None,
                            },
                        ],
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
        };

        let deser: IcebergSchemaV2 = serde_json::from_str(data).unwrap();
        assert_eq!(
            IcebergSchemaV2 {
                schema_id: 123,
                identifier_field_ids: Some(vec![1, 2, 3]),
                schema: ib_struct
            },
            deser
        );
    }

    #[test]
    fn test_iceberg_schema_v2_serde_roundtrip() {
        let ib_struct = StructType {
            fields: vec![
                StructField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: IcebergType::Primitive(PrimitiveType::Uuid),
                    doc: None,
                    initial_default: Some("0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb".to_string()),
                    write_default: Some("ec5911be-b0a7-458c-8438-c9a3e53cffae".to_string()),
                },
                StructField {
                    id: 2,
                    name: "list_data".to_string(),
                    required: false,
                    field_type: IcebergType::List(ListType {
                        element_id: 3,
                        element_required: true,
                        element: Box::new(IcebergType::Primitive(PrimitiveType::String)),
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 3,
                    name: "map_data".to_string(),
                    required: true,
                    field_type: IcebergType::Map(MapType {
                        key_id: 4,
                        key: Box::new(IcebergType::Primitive(PrimitiveType::Decimal {
                            precision: 30,
                            scale: 20,
                        })),
                        value_id: 5,
                        value_required: false,
                        value: Box::new(IcebergType::Primitive(PrimitiveType::Double)),
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                StructField {
                    id: 4,
                    name: "struct_data".to_string(),
                    required: true,
                    field_type: IcebergType::Struct(StructType {
                        fields: vec![
                            StructField {
                                id: 1,
                                name: "id".to_string(),
                                required: true,
                                field_type: IcebergType::Primitive(PrimitiveType::Long),
                                doc: None,
                                initial_default: None,
                                write_default: None,
                            },
                            StructField {
                                id: 2,
                                name: "list_fixed".to_string(),
                                required: false,
                                field_type: IcebergType::List(ListType {
                                    element_id: 3,
                                    element_required: true,
                                    element: Box::new(IcebergType::Primitive(
                                        PrimitiveType::Fixed(400),
                                    )),
                                }),
                                doc: None,
                                initial_default: None,
                                write_default: None,
                            },
                        ],
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
        };

        let schema = IcebergSchemaV2 {
            schema_id: 123,
            identifier_field_ids: Some(vec![1, 2, 3]),
            schema: ib_struct,
        };

        let ser = serde_json::to_string(&schema).unwrap();
        let deser: IcebergSchemaV2 = serde_json::from_str(&ser).unwrap();
        assert_eq!(schema, deser);
    }
}
