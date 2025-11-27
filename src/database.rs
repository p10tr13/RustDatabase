use std::collections::{BTreeMap, HashMap};
use crate::commands::{Command, CreateTableCommand, InsertCommand, SelectCommand, DeleteCommand};
use crate::domain::{DataType, DatabaseKey, Record};
use crate::error::{DbError, DbResult};
use crate::queries::Query;

pub struct Database<K: DatabaseKey> {
    tables: HashMap<String, Table<K>>,
}

impl<K: DatabaseKey> Database<K> {
    pub fn new() -> Database<K> {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, table: Table<K>) -> DbResult<()> {
        if !self.tables.contains_key(&table.name) {
            self.tables.insert(table.name.to_string(), table);
            Ok(())
        } else {
            Err(DbError::TableAlreadyExists("Table already exists".to_string()))
        }
    }

    pub fn get_table(&mut self, table: &str) -> DbResult<&Table<K>> {
        self.tables.get(table).ok_or_else(|| DbError::TableNotFound(table.to_string()))
    }

    pub fn get_table_mut(&mut self, table: &str) -> DbResult<&mut Table<K>> {
        self.tables.get_mut(table).ok_or_else(|| DbError::TableNotFound(table.to_string()))
    }
}

pub struct Table<K: DatabaseKey> {
    pub name: String,
    pk_name: String,
    schema: HashMap<String, DataType>,
    pub store: BTreeMap<K, Record>
}

impl<K: DatabaseKey> Table<K> {
    pub fn new(name: String, schema: HashMap<String, DataType>, pk_name: String) -> Table<K> {
        Self {
            name,
            schema,
            pk_name,
            store: BTreeMap::new()
        }
    }

    pub fn insert(&mut self, record: Record) -> DbResult<()> {
        record.validate(&self.schema)?;

        let pk_value = record.fields.get(self.pk_name.as_str()).ok_or_else(|| {
            DbError::ColumnNotFound(format!("Primary key {} not found", self.pk_name.clone()))
        })?;

        let key = K::from_value(pk_value).ok_or(DbError::KeyMismatch)?;

        if self.store.contains_key(&key) {
            return Err(DbError::DuplicateKey);
        }

        self.store.insert(key, record);
        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> Option<Record> {
        self.store.remove(key)
    }

    pub fn scan(&self) -> std::collections::btree_map::Values<'_ ,K, Record> {
        self.store.values()
    }
}

pub enum AnyDatabase {
    IntDatabase(Database<i64>),
    StringDatabase(Database<String>),
}

impl AnyDatabase {
    pub fn execute(&mut self, query: Query) -> DbResult<Option<String>> {
        match self {
            AnyDatabase::IntDatabase(database) => run_generic_query(database, query),
            AnyDatabase::StringDatabase(database) => run_generic_query(database, query),
        }
    }
}

fn run_generic_query<K: DatabaseKey>(database: &mut Database<K>, query: Query) -> DbResult<Option<String>> {
    match query {
        Query::Create { table, pk, columns} => {
            let schema: HashMap<_, _> = columns.into_iter().collect();
            let mut cmd = CreateTableCommand {database, name: table, pk_name: pk, schema};
            cmd.execute()
        },
        Query::Insert { table, values} => {
            let table = database.get_table_mut(&table)?;
            let record = Record {fields: values.into_iter().collect()};
            let mut cmd = InsertCommand {table, record};
            cmd.execute()
        },
        Query::Select { table, fields, condition } => {
            let table = database.get_table(&table)?;
            let mut cmd = SelectCommand {table, fields, condition};
            cmd.execute()
        },
        Query::Delete { table, key_value } => {
            let key = K::from_value(&key_value).ok_or(DbError::KeyMismatch)?;
            let t = database.get_table_mut(&table)?;
            let mut cmd = DeleteCommand { table: t, key };
            cmd.execute()
        },
        _ => Ok(None)
    }
}