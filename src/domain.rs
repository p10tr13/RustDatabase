use std::collections::HashMap;
use std::fmt;
use crate::error::{DbError, DbResult};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i),
            Value::String(s) => write!(f, "{}", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Float(fl) => write!(f, "{}", fl),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Bool,
    Int,
    Float,
    String,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub fields: HashMap<String, Value>,
}

impl Record {
    pub fn validate(&self, schema: &HashMap<String, DataType>) -> DbResult<()> {
        for (col_name, col_type) in schema {
            match self.fields.get(col_name) {
                Some(val) => Self::check_type(val, col_type)?,
                None => return Err(DbError::ColumnNotFound(col_name.clone()))
            }
        }
        Ok(())
    }

    fn check_type(val: &Value, col_type: &DataType) -> DbResult<()> {
        let valid = match (val, col_type) {
            (Value::Bool(_), DataType::Bool) => true,
            (Value::Int(_), DataType::Int) => true,
            (Value::String(_), DataType::String) => true,
            (Value::Float(_), DataType::Float) => true,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(DbError::TypeMismatch("Invalid type".to_string()))
        }
    }
}

pub trait DatabaseKey: Ord + Clone + fmt::Debug {
    fn from_value(val: &Value) -> Option<Self>;
}

impl DatabaseKey for i64 {
    fn from_value(val: &Value) -> Option<Self> {
        if let Value::Int(n) = val {
            Some(*n)
        } else {
            None
        }
    }
}

impl DatabaseKey for String {
    fn from_value(val: &Value) -> Option<Self> {
        if let Value::String(s) = val {
            Some(s.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod domain_tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_record_validation_fail() {
        let mut schema = HashMap::new();
        schema.insert("col_a".to_string(), DataType::Int);

        let mut fields = HashMap::new();
        fields.insert("col_a".to_string(), Value::String("tekst".to_string()));
        let record = Record { fields };

        let result = record.validate(&schema);

        match result {
            Err(DbError::TypeMismatch(_)) => assert!(true),
            Ok(_) => assert!(false, "Check_type should reject String in Int column"),
            Err(e) => assert!(false, "Expected TypeMismatch, got: {:?}", e),
        }
    }
}