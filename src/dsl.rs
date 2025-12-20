use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Pipeline {
    pub steps: Vec<Step>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Step {
    Select(Select),
    Filter(Filter),
    Cast(Cast),
    Sort(Sort),
    Join(Join),
    GroupBy(GroupBy),
    Window(Window),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Select {
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Filter {
    pub condition: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Cast {
    pub columns: HashMap<String, String>,
}

/// Sort: Order rows by one or more columns
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Sort {
    pub by: Vec<String>,
    #[serde(default)]
    pub descending: Vec<bool>,
}

/// Join: Combine two DataFrames
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Join {
    pub right_path: String,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
    #[serde(default = "default_join_how")]
    pub how: String,
}

fn default_join_how() -> String {
    "inner".to_string()
}

/// GroupBy: Aggregate data by groups
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct GroupBy {
    pub by: Vec<String>,
    pub aggs: HashMap<String, Agg>,
}

/// Aggregation function specification
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Agg {
    pub func: String,
    pub alias: Option<String>,
}

/// Window: Window/rolling functions
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Window {
    pub partition_by: Vec<String>,
    pub order_by: Option<String>,
    pub ops: Vec<WindowOp>,
}

/// Window operation specification
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct WindowOp {
    pub column: String,
    pub func: String,
    pub alias: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_select() {
        let yaml = r#"
steps:
  - type: select
    columns: ["a", "b"]
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::Select(s) => assert_eq!(s.columns, vec!["a", "b"]),
            _ => panic!("Expected Select step"),
        }
    }

    #[test]
    fn test_deserialize_filter() {
        let yaml = r#"
steps:
  - type: filter
    condition: "col('a') > 10"
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::Filter(f) => assert_eq!(f.condition, "col('a') > 10"),
            _ => panic!("Expected Filter step"),
        }
    }

    #[test]
    fn test_deserialize_cast() {
        let yaml = r#"
steps:
  - type: cast
    columns:
      a: "Int64"
      b: "Float32"
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::Cast(c) => {
                assert_eq!(c.columns.get("a").unwrap(), "Int64");
                assert_eq!(c.columns.get("b").unwrap(), "Float32");
            }
            _ => panic!("Expected Cast step"),
        }
    }

    #[test]
    fn test_deserialize_sort() {
        let yaml = r#"
steps:
  - type: sort
    by: ["date", "value"]
    descending: [false, true]
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::Sort(s) => {
                assert_eq!(s.by, vec!["date", "value"]);
                assert_eq!(s.descending, vec![false, true]);
            }
            _ => panic!("Expected Sort step"),
        }
    }

    #[test]
    fn test_deserialize_sort_default_ascending() {
        let yaml = r#"
steps:
  - type: sort
    by: ["date"]
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::Sort(s) => {
                assert_eq!(s.by, vec!["date"]);
                assert!(s.descending.is_empty()); // Default empty = all ascending
            }
            _ => panic!("Expected Sort step"),
        }
    }

    #[test]
    fn test_deserialize_join() {
        let yaml = r#"
steps:
  - type: join
    right_path: "lookup.csv"
    left_on: ["id"]
    right_on: ["user_id"]
    how: "left"
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::Join(j) => {
                assert_eq!(j.right_path, "lookup.csv");
                assert_eq!(j.left_on, vec!["id"]);
                assert_eq!(j.right_on, vec!["user_id"]);
                assert_eq!(j.how, "left");
            }
            _ => panic!("Expected Join step"),
        }
    }

    #[test]
    fn test_deserialize_join_default_inner() {
        let yaml = r#"
steps:
  - type: join
    right_path: "lookup.parquet"
    left_on: ["id"]
    right_on: ["id"]
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::Join(j) => {
                assert_eq!(j.how, "inner"); // Default
            }
            _ => panic!("Expected Join step"),
        }
    }

    #[test]
    fn test_deserialize_groupby() {
        let yaml = r#"
steps:
  - type: group_by
    by: ["category"]
    aggs:
      value:
        func: "sum"
        alias: "total_value"
      count:
        func: "count"
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::GroupBy(g) => {
                assert_eq!(g.by, vec!["category"]);
                let value_agg = g.aggs.get("value").unwrap();
                assert_eq!(value_agg.func, "sum");
                assert_eq!(value_agg.alias, Some("total_value".to_string()));
                let count_agg = g.aggs.get("count").unwrap();
                assert_eq!(count_agg.func, "count");
                assert_eq!(count_agg.alias, None);
            }
            _ => panic!("Expected GroupBy step"),
        }
    }

    #[test]
    fn test_deserialize_window() {
        let yaml = r#"
steps:
  - type: window
    partition_by: ["category"]
    order_by: "date"
    ops:
      - column: "value"
        func: "sum"
        alias: "running_sum"
      - column: "id"
        func: "rank"
        alias: "rank_in_category"
"#;
        let pipeline: Pipeline = serde_yaml::from_str(yaml).unwrap();
        match &pipeline.steps[0] {
            Step::Window(w) => {
                assert_eq!(w.partition_by, vec!["category"]);
                assert_eq!(w.order_by, Some("date".to_string()));
                assert_eq!(w.ops.len(), 2);
                assert_eq!(w.ops[0].column, "value");
                assert_eq!(w.ops[0].func, "sum");
                assert_eq!(w.ops[0].alias, "running_sum");
            }
            _ => panic!("Expected Window step"),
        }
    }
}
