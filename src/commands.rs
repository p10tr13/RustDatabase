use std::collections::HashMap;
use crate::database::{Database, Table};
use crate::domain::{DatabaseKey, Record, Value, DataType};
use crate::error::{DbResult, DbError};

pub trait Command {
    fn execute(&mut self) -> DbResult<Option<String>>;
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug)]
pub struct Condition {
    pub column: String,
    pub operator: Operator,
    pub value: Value,
}

pub struct SelectCommand<'a, K: DatabaseKey> {
    pub condition: Option<Condition>,
    pub table: &'a Table<K>,
    pub fields: Vec<String>,
}

impl<'a, K: DatabaseKey> Command for SelectCommand<'a, K> {
    fn execute(&mut self) -> DbResult<Option<String>> {
        let mut rows = Vec::new();
        for record in self.table.scan() {
            if let Some(condition) = &self.condition {
                let value = record.fields.get(&condition.column)
                    .ok_or(DbError::ColumnNotFound(condition.column.clone()))?;
                if !evaluate_condition(value, &condition.value, &condition.operator) {
                    continue;
                }
            }
            let mut row_strings = Vec::new();
            for field in &self.fields {
                let val = record.fields.get(field)
                    .ok_or_else(|| DbError::ColumnNotFound(field.clone()))?;
                row_strings.push(format!("{}", val));
            }
            rows.push(row_strings.join(", "));
        }
        Ok(Some(rows.join("\n")))
    }
}

fn evaluate_condition(value1: &Value, value2: &Value, operator: &Operator) -> bool {
    match operator {
        Operator::Equal => value1 == value2,
        Operator::NotEqual => value1 != value2,
        Operator::GreaterThan => value1 > value2,
        Operator::GreaterThanOrEqual => value1 >= value2,
        Operator::LessThan => value1 < value2,
        Operator::LessThanOrEqual => value1 <= value2,
    }
}

pub struct CreateTableCommand<'a, K: DatabaseKey> {
    pub database: &'a mut Database<K>,
    pub name: String,
    pub pk_name: String,
    pub schema: HashMap<String, DataType>,
}

impl<'a, K: DatabaseKey> Command for CreateTableCommand<'a, K> {
    fn execute(&mut self) -> DbResult<Option<String>> {
        let table = Table::new(
            self.name.clone(),
            self.schema.clone(),
            self.pk_name.clone(),
        );
        self.database.create_table(table)?;
        Ok(Some(format!("Table {} created.", self.name)))
    }
}

pub struct InsertCommand<'a, K: DatabaseKey> {
    pub table: &'a mut Table<K>,
    pub record: Record,
}

impl<'a, K: DatabaseKey> Command for InsertCommand<'a, K> {
    fn execute(&mut self) -> DbResult<Option<String>> {
        self.table.insert(self.record.clone())?;
        Ok(Some("Record inserted".to_string()))
    }
}

pub struct DeleteCommand<'a, K: DatabaseKey> {
    pub table: &'a mut Table<K>,
    pub key: K,
}

impl<'a, K: DatabaseKey> Command for DeleteCommand<'a, K> {
    fn execute(&mut self) -> DbResult<Option<String>> {
        match self.table.delete(&self.key) {
            Some(_) => Ok(Some("Deleted record".to_string())),
            None => Err(DbError::KeyMismatch)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Database<i64> {
        Database::new()
    }

    fn get_people_schema() -> HashMap<String, DataType> {
        let mut s = HashMap::new();
        s.insert("id".into(), DataType::Int);
        s.insert("job".into(), DataType::String);
        s.insert("height".into(), DataType::Float);
        s.insert("age".into(), DataType::Int);
        s.insert("sex".into(), DataType::String);
        s
    }

    #[test]
    fn test_exec_select() {
        let mut t = Table::new("people".into(), get_people_schema(),"id".into());

        let mut f1 = HashMap::new();
        f1.insert("id".into(), Value::Int(1));
        f1.insert("sex".into(), Value::String("male".into()));
        f1.insert("job".into(), Value::String("actor".into()));
        f1.insert("height".into(), Value::Float(180.0));
        f1.insert("age".into(), Value::Int(30));
        t.store.insert(1, Record { fields: f1 });

        let mut f2 = HashMap::new();
        f2.insert("id".into(), Value::Int(2));
        f2.insert("sex".into(), Value::String("female".into()));
        f2.insert("job".into(), Value::String("actress".into()));
        f2.insert("height".into(), Value::Float(170.0));
        f2.insert("age".into(), Value::Int(25));
        t.store.insert(2, Record { fields: f2 });

        let cond = Condition {
            column: "sex".into(),
            operator: Operator::Equal,
            value: Value::String("male".into()),
        };

        let mut cmd = SelectCommand {
            table: &t,
            fields: vec!["job".into()],
            condition: Some(cond),
        };

        match cmd.execute() {
            Ok(Some(output)) => {
                assert!(output.contains("actor"));
                assert!(!output.contains("actress"));
            },
            _ => assert!(false, "SELECT execute error"),
        }
    }

    #[test]
    fn test_exec_select_no_where() {
        let mut t = Table::new("people".into(), get_people_schema(), "id".into());
        let mut f1 = HashMap::new();
        f1.insert("id".into(), Value::Int(1));
        t.store.insert(1, Record { fields: f1 });

        let mut cmd = SelectCommand {
            table: &t,
            fields: vec!["id".into()],
            condition: None,
        };

        match cmd.execute() {
            Ok(Some(output)) => assert!(output.contains("1")),
            _ => assert!(false, "SELECT without WHERE error"),
        }
    }

    #[test]
    fn test_exec_create() {
        let mut db = setup_db();
        let mut cmd = CreateTableCommand {
            database: &mut db,
            name: "people".into(),
            pk_name: "id".into(),
            schema: get_people_schema(),
        };

        let res = cmd.execute();
        assert!(res.is_ok());
        assert!(db.get_table("people").is_ok());
    }

    #[test]
    fn test_exec_insert() {
        let mut db = setup_db();
        let t = Table::new("people".into(), get_people_schema(), "id".into());

        if let Err(e) = db.create_table(t) {
            assert!(false, "Not able to create a table: {:?}", e);
            return;
        }

        match db.get_table_mut("people") {
            Ok(t_ref) => {
                let mut fields = HashMap::new();
                fields.insert("id".into(), Value::Int(1));
                fields.insert("job".into(), Value::String("fire fighter".into()));
                fields.insert("height".into(), Value::Float(180.0));
                fields.insert("age".into(), Value::Int(30));
                fields.insert("sex".into(), Value::String("male".into()));

                let mut cmd = InsertCommand {
                    table: t_ref,
                    record: Record { fields },
                };

                if let Err(e) = cmd.execute() {
                    assert!(false, "INSERT execute error: {:?}", e);
                }

                if let Some(rec) = t_ref.store.get(&1) {
                    assert_eq!(
                        rec.fields.get("job"),
                        Some(&Value::String("fire fighter".into())),
                        "The record fields do not match the inserted ones"
                    );
                } else {
                    assert!(false, "Record not found after INSERT");
                }
            },
            Err(e) => {
                assert!(false, "Not able to get 'people' table: {:?}", e);
            }
        }
    }

    #[test]
    fn test_exec_delete() {
        let mut t = Table::new("people".into(), get_people_schema(), "id".into());
        let mut f1 = HashMap::new();
        f1.insert("id".into(), Value::Int(5));
        t.store.insert(5, Record { fields: f1 });

        let mut cmd = DeleteCommand {
            table: &mut t,
            key: 5,
        };

        assert!(cmd.execute().is_ok());
        assert!(t.store.get(&5).is_none());
    }

    #[test]
    fn test_insert_duplicate_key() {

        let mut table = Table::new(
            "users".into(),
            HashMap::from([("id".to_string(), DataType::Int)]),
            "id".into()
        );

        let rec1 = Record {
            fields: HashMap::from([("id".to_string(), Value::Int(100))])
        };

        table.store.insert(100, rec1.clone());

        let mut cmd = InsertCommand {
            table: &mut table,
            record: rec1,
        };

        let result = cmd.execute();

        match result {
            Err(DbError::DuplicateKey) => assert!(true),
            Ok(_) => assert!(false, "There should be duplicate key error"),
            Err(e) => assert!(false, "DuplicateKey error expected, got: {:?}", e),
        }
    }
}