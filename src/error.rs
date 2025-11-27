use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Table '{0}' not found.")]
    TableNotFound(String),
    #[error("Table '{0}' already exists.")]
    TableAlreadyExists(String),
    #[error("Column '{0}' not found.")]
    ColumnNotFound(String),
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Key mismatch")]
    KeyMismatch,
    #[error("Duplicate key")]
    DuplicateKey,
    #[error("Syntax error: {0}")]
    SyntaxError(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Command error: {0}")]
    CommandError(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

pub type DbResult<T> = Result<T, DbError>;