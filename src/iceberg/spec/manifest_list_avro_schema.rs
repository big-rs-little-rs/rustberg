pub(crate) const MANIFEST_LIST_V2_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "manifest_list",
    "fields": [
        {
            "name": "manifest_path",
            "type": "string",
            "field_id": 500
        },
        {
            "name": "manifest_length",
            "type": "long",
            "field_id": 501
        },
        {
            "name": "partition_spec_id",
            "type": "int",
            "field_id": 502
        },
        {
            "name": "content",
            "type": "int",
            "field_id": 517,
            "default": 0
        },
        {
            "name": "sequence_number",
            "type": "long",
            "field_id": 515,
            "default": 0
        },
        {
            "name": "min_sequence_number",
            "type": "long",
            "field_id": 516,
            "default": 0
        },
        {
            "name": "added_snapshot_id",
            "type": "long",
            "default": null,
            "field_id": 503
        },
        {
            "name": "added_files_count",
            "type": "int",
            "field_id": 504,
            "default": 0
        },
        {
            "name": "existing_files_count",
            "type": "int",
            "field_id": 505,
            "default": 0
        },
        {
            "name": "deleted_files_count",
            "type": "int",
            "field_id": 506,
            "default": 0
        },
        {
            "name": "added_rows_count",
            "type": "long",
            "field_id": 512,
            "default": 0
        },
        {
            "name": "existing_rows_count",
            "type": "long",
            "field_id": 513,
            "default": 0
        },
        {
            "name": "deleted_rows_count",
            "type": "long",
            "field_id": 514,
            "default": 0
        },
        {
            "name": "partitions",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "field_summary",
                        "fields": [
                            {
                                "name": "contains_null",
                                "type": "boolean",
                                "field_id": 509
                            },
                            {
                                "name": "contains_nan",
                                "type": [
                                    "null",
                                    "boolean"
                                ],
                                "field_id": 518,
                                "default": null
                            },
                            {
                                "name": "lower_bound",
                                "type": [
                                    "null",
                                    "bytes"
                                ],
                                "field_id": 510,
                                "default": null
                            },
                            {
                                "name": "upper_bound",
                                "type": [
                                    "null",
                                    "bytes"
                                ],
                                "field_id": 511,
                                "default": null
                            }
                        ]
                    },
                    "element-id": 508
                }
            ],
            "default": null,
            "field_id": 507
        },
        {
            "name": "key_metadata",
            "type": [
                "null",
                "bytes"
            ],
            "field_id": 519,
            "default": null
        }
    ]
}
"#;

pub(crate) const MANIFEST_LIST_V1_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "manifest_list",
    "fields": [
        {
            "name": "manifest_path",
            "type": "string",
            "field_id": 500
        },
        {
            "name": "manifest_length",
            "type": "long",
            "field_id": 501
        },
        {
            "name": "partition_spec_id",
            "type": "int",
            "field_id": 502
        },
        {
            "name": "added_snapshot_id",
            "type": "long",
            "default": null,
            "field_id": 503
        },
        {
            "name": "added_files_count",
            "type": [
                "null",
                "int"
            ],
            "default": null,
            "field_id": 504
        },
        {
            "name": "existing_files_count",
            "type": [
                "null",
                "int"
            ],
            "default": null,
            "field_id": 505
        },
        {
            "name": "deleted_files_count",
            "type": [
                "null",
                "int"
            ],
            "default": null,
            "field_id": 506
        },
        {
            "name": "added_rows_count",
            "type": [
                "null",
                "long"
            ],
            "default": null,
            "field_id": 512
        },
        {
            "name": "existing_rows_count",
            "type": [
                "null",
                "long"
            ],
            "default": null,
            "field_id": 513
        },
        {
            "name": "deleted_rows_count",
            "type": [
                "null",
                "long"
            ],
            "default": null,
            "field_id": 514
        },
        {
            "name": "partitions",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "field_summary",
                        "fields": [
                            {
                                "name": "contains_null",
                                "type": "boolean",
                                "field_id": 509
                            },
                            {
                                "name": "contains_nan",
                                "type": [
                                    "null",
                                    "boolean"
                                ],
                                "field_id": 518,
                                "default": null
                            },
                            {
                                "name": "lower_bound",
                                "type": [
                                    "null",
                                    "bytes"
                                ],
                                "field_id": 510,
                                "default": null
                            },
                            {
                                "name": "upper_bound",
                                "type": [
                                    "null",
                                    "bytes"
                                ],
                                "field_id": 511,
                                "default": null
                            }
                        ]
                    },
                    "element-id": 508
                }
            ],
            "default": null,
            "field_id": 507
        },
        {
            "name": "key_metadata",
            "type": [
                "null",
                "bytes"
            ],
            "field_id": 519,
            "default": null
        }
    ]
}
"#;
