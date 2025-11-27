use clap::Parser;
use std::{fs, io};
use std::io::Write;
use rust_database_project::{
    database::{AnyDatabase, Database},
    queries::{parse, Query},
    error::DbError,
};

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "string")]
    key_type: String,
}

fn main() {
    let args = Args::parse();

    let mut db = match args.key_type.as_str() {
        "int" => AnyDatabase::IntDatabase(Database::new()),
        _ => AnyDatabase::StringDatabase(Database::new()),
    };

    println!("Database is ready (Key type: {}).", args.key_type);

    let stdin = io::stdin();
    let mut buffer = String::new();
    let mut history = Vec::new();

    loop {
        print!("> ");
        io::stdout().flush().ok();
        buffer.clear();

        if stdin.read_line(&mut buffer).is_err() { break; }
        let input = buffer.trim();
        if input.is_empty() { continue; }
        if input.eq_ignore_ascii_case("exit") || input.eq_ignore_ascii_case("quit") {
            break;
        }

        if let Err(e) = process_command(&mut db, input, &mut history) {
            eprintln!("Error: {}", e);
        }
    }
}

fn process_command(db: &mut AnyDatabase, input: &str, history: &mut Vec<String>) -> Result<(), DbError> {
    let query = parse(input).map_err(|e| DbError::SyntaxError(e.to_string()))?;

    match query {
        Query::SaveAs(path) => {
            fs::write(&path, history.join("\n"))?;
            println!("Saved history to: {}", path);
        }
        Query::ReadFrom(path) => {
            let content = fs::read_to_string(&path)?;
            for line in content.lines() {
                if !line.trim().is_empty() {
                    println!("FILE> {}", line);
                    process_command(db, line, history)?;
                }
            }
        }
        _ => {
            if let Some(result) = db.execute(query)? {
                println!("{}", result);
            }
            history.push(input.to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_process_save_as() {
        let path_str = "test_dump_save.txt";
        cleanup(path_str);

        let mut db = AnyDatabase::StringDatabase(Database::new());
        let mut history = vec![
            "CREATE t KEY k FIELDS k:String".to_string(),
            "INSERT k=\"x\" INTO t".to_string()
        ];

        let cmd = format!("SAVE_AS {}", path_str);
        let res = process_command(&mut db, &cmd, &mut history);

        assert!(res.is_ok(), "process_command returned error: {:?}", res.err());

        assert!(Path::new(path_str).exists(), "File not created");
        match fs::read_to_string(path_str) {
            Ok(content) => {
                let expected = "CREATE t KEY k FIELDS k:String\nINSERT k=\"x\" INTO t";
                assert_eq!(content.trim(), expected);
            }
            Err(e) => assert!(false, "Cannot read the file: {}", e),
        }

        cleanup(path_str);
    }

    #[test]
    fn test_process_read_from() {
        let path_str = "test_script_read.txt";
        cleanup(path_str);

        let script_content = "CREATE users KEY id FIELDS id:String, age:Int\nINSERT id=\"u1\", age=20 INTO users";
        if let Err(e) = fs::write(path_str, script_content) {
            assert!(false, "Error while setting up a test (file write): {}", e);
        }

        let mut db = AnyDatabase::StringDatabase(Database::new());
        let mut history = Vec::new();

        let cmd = format!("READ_FROM {}", path_str);
        let res = process_command(&mut db, &cmd, &mut history);

        assert!(res.is_ok(), "process_command returned error: {:?}", res.err());

        assert_eq!(history.len(), 2, "History should have 2 commands from file");
        assert!(history[0].contains("CREATE users"));
        assert!(history[1].contains("INSERT id=\"u1\""));

        cleanup(path_str);
    }
}