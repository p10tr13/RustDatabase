use crate::domain::{Value, DataType};
use crate::commands::{Operator, Condition};
use pest::Parser;
use pest_derive::Parser;
use crate::error::{DbError, DbResult};

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct QueryParser;

#[derive(Debug)]
pub enum Query {
    Select {
        table: String,
        fields: Vec<String>,
        condition: Option<Condition>,
    },
    Create {
        table: String,
        pk: String,
        columns: Vec<(String, DataType)>,
    },
    Insert {
        table: String,
        values: Vec<(String, Value)>,
    },
    Delete {
        table: String,
        key_value: Value,
    },
    SaveAs(String),
    ReadFrom(String),
}

pub fn parse(input: &str) -> DbResult<Query> {
    let mut pairs = QueryParser::parse(Rule::query, input)
        .map_err(|e| DbError::SyntaxError(e.to_string()))?;
    let pair = pairs.next().ok_or(DbError::SyntaxError("Invalid query format".into()))?;

    match pair.as_rule() {
        Rule::select_cmd => parse_select_command(pair),
        Rule::create_cmd => parse_create_command(pair),
        Rule::delete_cmd => parse_delete_command(pair),
        Rule::insert_cmd => parse_insert_command(pair),
        Rule::save_cmd => pair.into_inner().next()
            .map(|p| Query::SaveAs(p.as_str().to_string()))
            .ok_or(DbError::InvalidPath("No path".into())),
        Rule::read_cmd => pair.into_inner().next()
            .map(|p| Query::ReadFrom(p.as_str().to_string()))
            .ok_or(DbError::InvalidPath("No path".into())),
        _ => Err(DbError::SyntaxError("Invalid query format".into())),
    }
}

fn parse_select_command(pair: pest::iterators::Pair<Rule>) -> DbResult<Query> {
    let inner = pair.into_inner();
    let mut fields = Vec::new();
    let mut cond = None;
    for p in inner {
        match p.as_rule() {
            Rule::ident => fields.push(p.as_str().to_string()),
            Rule::where_clause => cond = Some(parse_where(p)?),
            _ => {}
        }
    }
    let table = fields.pop().ok_or(DbError::SyntaxError("No table in SELECT".into()))?;
    Ok(Query::Select { table, fields, condition: cond })
}

fn parse_where(pair: pest::iterators::Pair<Rule>) -> DbResult<Condition> {
    let mut inner = pair.into_inner();
    let column = inner.next().map(|p| p.as_str().to_string())
        .ok_or(DbError::SyntaxError("No column in WHERE".into()))?;
    let op = inner.next().map(|p| p.as_str())
        .ok_or(DbError::SyntaxError("No operator".into()))?;
    let value = parse_value(inner.next().ok_or(DbError::SyntaxError("No value in WHERE".into()))?)?;
    let operator = match op {
        "=" => Operator::Equal,
        "!=" => Operator::NotEqual,
        "<=" => Operator::LessThanOrEqual,
        ">=" => Operator::GreaterThanOrEqual,
        "<" => Operator::LessThan,
        ">" => Operator::GreaterThan,
        _ => return Err(DbError::SyntaxError("Invalid operator".into())),
    };
    Ok(Condition {column, operator, value})
}

fn parse_value(pair: pest::iterators::Pair<Rule>) -> DbResult<Value> {
    let mut it = pair.into_inner();
    let inner = it.next().ok_or(DbError::SyntaxError("No value in VALUE".into()))?;
    match inner.as_rule() {
        Rule::int_w => inner.as_str().parse().map(Value::Int)
            .map_err(|_| DbError::SyntaxError("Bad Int".into())),
        Rule::float_w => inner.as_str().parse().map(Value::Float)
            .map_err(|_| DbError::SyntaxError("Bad Float".into())),
        Rule::bool_w => Ok(Value::Bool(inner.as_str()=="true")),
        Rule::string_w => {
            let s = inner.as_str();
            if s.len()>=2 { Ok(Value::String(s[1..s.len()-1].to_string())) }
            else { Err(DbError::SyntaxError("Bad string literal".into())) }
        }
        _ => Err(DbError::SyntaxError("Unknown type of the value".into())),
    }
}

fn parse_create_command(pair: pest::iterators::Pair<Rule>) -> DbResult<Query> {
    let mut inner = pair.into_inner();
    let table= inner.next().map(|x| x.as_str().to_string())
        .ok_or(DbError::SyntaxError("No table in CREATE".into()))?;
    let pk = inner.next().map(|x| x.as_str().to_string())
        .ok_or(DbError::SyntaxError("No primary key in CREATE".into()))?;

    let mut cols = Vec::new();
    for column in inner {
        let mut definiftion = column.into_inner();
        let name = definiftion.next().map(|x| x.as_str().to_string())
            .ok_or(DbError::SyntaxError("No name for column in CREATE".into()))?;
        let typ = definiftion.next().map(|x| x.as_str())
            .ok_or(DbError::SyntaxError("No type in CREATE".into()))?;
        let dtype = match typ {
            "Int" => DataType::Int,
            "Float" => DataType::Float,
            "Bool" => DataType::Bool,
            "String" => DataType::String,
            _ => return Err(DbError::SyntaxError("Unknown type in CREATE".into())),
        };
        cols.push((name, dtype));
    }
    Ok(Query::Create {table, pk, columns: cols})
}

fn parse_delete_command(pair: pest::iterators::Pair<Rule>) -> DbResult<Query> {
    let mut inner = pair.into_inner();
    let v = inner.next().ok_or(DbError::SyntaxError("No value in DELETE".into()))?;
    let table = inner.next().map(|x| x.as_str().to_string())
        .ok_or(DbError::SyntaxError("No table in DELETE".into()))?;
    let value = parse_value(v)?;
    Ok(Query::Delete {table, key_value: value})
}

fn parse_insert_command(pair: pest::iterators::Pair<Rule>) -> DbResult<Query> {
    let inner = pair.into_inner();
    let mut values = Vec::new();
    let mut table = None;
    for p in inner {
        match p.as_rule() {
            Rule::assigment => {
                let mut a = p.into_inner();
                let column = a.next().map(|x| x.as_str().to_string())
                    .ok_or(DbError::SyntaxError("No column name in INSERT".into()))?;
                let value = parse_value(a.next()
                    .ok_or(DbError::SyntaxError("No value in INSERT".into()))?)?;
                values.push((column, value));
            }
            Rule::ident => table = Some(p.as_str().to_string()),
            _ => return Err(DbError::SyntaxError("Unknown syntax of INSERT".into())),
        }
    }
    Ok(Query::Insert {table: table.ok_or(DbError::SyntaxError("No table name in INSERT".into()))?, values})
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_command() {
        let input = "SELECT job, height, age FROM people WHERE sex = \"male\"";
        match parse(input) {
            Ok(Query::Select { table, fields, condition }) => {
                assert_eq!(table, "people");
                assert_eq!(fields, vec!["job", "height", "age"]);
                if let Some(c) = condition {
                    assert_eq!(c.column, "sex");
                    assert_eq!(c.operator, Operator::Equal);
                    assert_eq!(c.value, Value::String("male".into()));
                } else {
                    assert!(false, "No where clause");
                }
            }
            _ => assert!(false, "SELECT parsing error"),
        }
    }

    #[test]
    fn test_parse_create() {
        let input = "CREATE people KEY id FIELDS id:Int, job:String, height:Float";
        match parse(input) {
            Ok(Query::Create { table, pk, columns }) => {
                assert_eq!(table, "people");
                assert_eq!(pk, "id");
                assert_eq!(columns.len(), 3);
            }
            _ => assert!(false, "CREATE parsing error"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let input = "DELETE 100 FROM people";
        match parse(input) {
            Ok(Query::Delete { table, key_value}) => {
                assert_eq!(table, "people");
                assert_eq!(key_value, Value::Int(100));
            }
            _ => assert!(false, "DELETE parsing error"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let input = "INSERT id=1, job=\"fire fighter\", height=180.5 INTO people";
        match parse(input) {
            Ok(Query::Insert { table, values }) => {
                assert_eq!(table, "people");
                assert_eq!(values.len(), 3);
                match &values[2].1 {
                    Value::Float(f) => assert!((f - 180.5).abs() < f64::EPSILON),
                    _ => assert!(false, "Float was expected"),
                }
            }
            _ => assert!(false, "INSERT parsing error"),
        }
    }

    #[test]
    fn test_parse_save() {
        let input = "SAVE_AS backup.db";
        match parse(input) {
            Ok(Query::SaveAs(path)) => assert_eq!(path, "backup.db"),
            _ => assert!(false, "SAVE_AS parsing error"),
        }
    }

    #[test]
    fn test_parse_read() {
        let input = "READ_FROM init.sql";
        match parse(input) {
            Ok(Query::ReadFrom(path)) => assert_eq!(path, "init.sql"),
            _ => assert!(false, "READ_FROM parsing error"),
        }
    }

    #[test]
    fn test_parse_invalid_syntax() {
        let input = "CREATE TABLE without KEY keyword";

        let result = parse(input);

        match result {
            Err(DbError::SyntaxError(_)) => assert!(true),
            Ok(_) => assert!(false, "Parser should have failed"),
            Err(e) => assert!(false, "SyntaxError expected, got: {:?}", e),
        }
    }
}