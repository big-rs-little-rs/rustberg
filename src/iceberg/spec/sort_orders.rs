use serde::{Deserialize, Serialize};

use super::partition_spec::Transform;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrders {
    pub order_id: i32,
    pub fields: Vec<SortField>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct SortField {
    pub transform: Transform,
    pub source_id: i32,
    pub direction: Direction,
    pub null_order: NullOrder,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    ASC,
    DESC,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_direction() {
        let ser = [r#""asc""#, r#""desc""#];
        let directions = ser.map(|ser| {
            serde_json::from_str::<Direction>(ser).expect(&format!("Failed for input {}", ser))
        });
        assert_eq!([Direction::ASC, Direction::DESC], directions);
    }

    #[test]
    fn test_wrong_direction_fails() {
        let ser = r#""dsc""#;
        let direction = serde_json::from_str::<Direction>(ser);
        assert!(direction.is_err());
    }

    #[test]
    fn test_null_order() {
        let ser = [r#""nulls-last""#, r#""nulls-first""#];
        let null_orders = ser.map(|ser| {
            serde_json::from_str::<NullOrder>(ser).expect(&format!("Failed for input {}", ser))
        });
        assert_eq!([NullOrder::NullsLast, NullOrder::NullsFirst], null_orders);
    }

    #[test]
    fn test_wrong_null_order_fails() {
        let ser = r#""nulls""#;
        let null_order = serde_json::from_str::<NullOrder>(ser);
        assert!(null_order.is_err());
    }
}
