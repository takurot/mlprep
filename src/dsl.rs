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
}
