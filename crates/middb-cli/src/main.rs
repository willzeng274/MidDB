use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use middb_core::{Config, Database};
use middb_network::{Client, Server};
use middb_query::{BinaryOperator, Executor, Expr, Planner, Row, Table, Value};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "middb")]
#[command(about = "MidDB command-line interface")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Server {
        #[arg(short, long, default_value = "./data")]
        data_dir: PathBuf,
        
        #[arg(short, long, default_value = "127.0.0.1:7878")]
        bind: String,
    },
    
    Client {
        #[arg(short, long, default_value = "127.0.0.1:7878")]
        server: String,
    },
    
    Local {
        #[arg(short, long, default_value = "./data")]
        data_dir: PathBuf,
    },
    
    Query {
        #[arg(short, long, default_value = "./data")]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Server { data_dir, bind } => {
            run_server(data_dir, bind).await
        }
        Commands::Client { server } => {
            run_client(&server).await
        }
        Commands::Local { data_dir } => {
            run_local(data_dir)
        }
        Commands::Query { data_dir } => {
            run_query(data_dir)
        }
    }
}

async fn run_server(data_dir: PathBuf, bind: String) -> Result<()> {
    println!("Starting MidDB server");
    println!("Data directory: {:?}", data_dir);
    println!("Binding to: {}", bind);
    
    let config = Config::new(data_dir);
    let db = Database::open(config).context("Failed to open database")?;
    
    let server = Server::new(db, bind.clone());
    println!("Server listening on {}", bind);
    
    server.run().await.context("Server error")?;
    
    Ok(())
}

async fn run_client(server: &str) -> Result<()> {
    println!("Connecting to {}", server);
    
    let mut client = Client::connect(server)
        .await
        .context("Failed to connect to server")?;
    
    client.ping().await.context("Ping failed")?;
    println!("Connected to server\n");
    
    let mut rl = DefaultEditor::new()?;
    
    println!("MidDB Client REPL");
    println!("Commands: get <key>, put <key> <value>, delete <key>, quit");
    println!();
    
    loop {
        let readline = rl.readline("middb> ");
        
        match readline {
            Ok(line) => {
                let line = line.trim();
                
                if line.is_empty() {
                    continue;
                }
                
                rl.add_history_entry(line)?;
                
                if line == "quit" || line == "exit" {
                    break;
                }
                
                if let Err(e) = handle_client_command(&mut client, line).await {
                    eprintln!("Error: {}", e);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error: {}", err);
                break;
            }
        }
    }
    
    println!("Goodbye");
    Ok(())
}

async fn handle_client_command(client: &mut Client, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    
    if parts.is_empty() {
        return Ok(());
    }
    
    match parts[0] {
        "get" => {
            if parts.len() != 2 {
                anyhow::bail!("Usage: get <key>");
            }
            
            let key = parts[1].as_bytes();
            match client.get(key).await? {
                Some(value) => {
                    println!("{}", String::from_utf8_lossy(&value));
                }
                None => {
                    println!("(nil)");
                }
            }
        }
        
        "put" => {
            if parts.len() < 3 {
                anyhow::bail!("Usage: put <key> <value>");
            }
            
            let key = parts[1].as_bytes();
            let value = parts[2..].join(" ");
            
            client.put(key, value.as_bytes()).await?;
            println!("OK");
        }
        
        "delete" | "del" => {
            if parts.len() != 2 {
                anyhow::bail!("Usage: delete <key>");
            }
            
            let key = parts[1].as_bytes();
            client.delete(key).await?;
            println!("OK");
        }
        
        "ping" => {
            client.ping().await?;
            println!("PONG");
        }
        
        _ => {
            anyhow::bail!("Unknown command: {}", parts[0]);
        }
    }
    
    Ok(())
}

fn run_local(data_dir: PathBuf) -> Result<()> {
    println!("Opening local database at {:?}", data_dir);
    
    let config = Config::new(data_dir);
    let db = Database::open(config).context("Failed to open database")?;
    
    println!("Database opened\n");
    
    let mut rl = DefaultEditor::new()?;
    
    println!("MidDB Local REPL");
    println!("Commands: get <key>, put <key> <value>, delete <key>, stats, quit");
    println!();
    
    loop {
        let readline = rl.readline("middb> ");
        
        match readline {
            Ok(line) => {
                let line = line.trim();
                
                if line.is_empty() {
                    continue;
                }
                
                rl.add_history_entry(line)?;
                
                if line == "quit" || line == "exit" {
                    break;
                }
                
                if let Err(e) = handle_local_command(&db, line) {
                    eprintln!("Error: {}", e);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error: {}", err);
                break;
            }
        }
    }
    
    println!("Closing database");
    db.close().context("Failed to close database")?;
    
    println!("Goodbye");
    Ok(())
}

fn handle_local_command(db: &Database, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    
    if parts.is_empty() {
        return Ok(());
    }
    
    match parts[0] {
        "get" => {
            if parts.len() != 2 {
                anyhow::bail!("Usage: get <key>");
            }
            
            let key = parts[1].as_bytes().to_vec();
            match db.get(&key)? {
                Some(value) => {
                    println!("{}", String::from_utf8_lossy(&value));
                }
                None => {
                    println!("(nil)");
                }
            }
        }
        
        "put" => {
            if parts.len() < 3 {
                anyhow::bail!("Usage: put <key> <value>");
            }
            
            let key = parts[1].as_bytes().to_vec();
            let value = parts[2..].join(" ");
            
            db.put(key, value.as_bytes().to_vec())?;
            println!("OK");
        }
        
        "delete" | "del" => {
            if parts.len() != 2 {
                anyhow::bail!("Usage: delete <key>");
            }
            
            let key = parts[1].as_bytes().to_vec();
            db.delete(key)?;
            println!("OK");
        }
        
        "stats" => {
            let stats = db.stats();
            println!("MemTable size: {} bytes", stats.memtable_size);
            println!("MemTable entries: {}", stats.memtable_entries);
            println!("SSTables: {}", stats.num_sstables);
            println!("Sequence: {}", stats.sequence_number);
        }
        
        _ => {
            anyhow::bail!("Unknown command: {}", parts[0]);
        }
    }
    
    Ok(())
}

fn run_query(_data_dir: PathBuf) -> Result<()> {
    println!("Query mode (in-memory tables for demonstration)\n");
    
    let mut executor = Executor::new();
    
    let mut users = Table::new("users".to_string());
    users.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(1)),
        ("name".to_string(), Value::String("Alice".to_string())),
        ("age".to_string(), Value::Int(30)),
    ]));
    users.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(2)),
        ("name".to_string(), Value::String("Bob".to_string())),
        ("age".to_string(), Value::Int(25)),
    ]));
    users.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(3)),
        ("name".to_string(), Value::String("Charlie".to_string())),
        ("age".to_string(), Value::Int(35)),
    ]));
    
    executor.register_table("users".to_string(), users);
    
    println!("Registered table 'users' with 3 rows\n");
    
    let mut rl = DefaultEditor::new()?;
    
    println!("Query REPL");
    println!("Commands: scan <table>, filter <table> <column> <op> <value>, quit");
    println!("Example: filter users age > 25\n");
    
    let planner = Planner::new();
    
    loop {
        let readline = rl.readline("query> ");
        
        match readline {
            Ok(line) => {
                let line = line.trim();
                
                if line.is_empty() {
                    continue;
                }
                
                rl.add_history_entry(line)?;
                
                if line == "quit" || line == "exit" {
                    break;
                }
                
                if let Err(e) = handle_query_command(&executor, &planner, line) {
                    eprintln!("Error: {}", e);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error: {}", err);
                break;
            }
        }
    }
    
    println!("Goodbye");
    Ok(())
}

fn handle_query_command(executor: &Executor, planner: &Planner, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    
    if parts.is_empty() {
        return Ok(());
    }
    
    match parts[0] {
        "scan" => {
            if parts.len() != 2 {
                anyhow::bail!("Usage: scan <table>");
            }
            
            let logical = planner.plan(parts[1].to_string(), None);
            let physical = planner.to_physical(logical);
            
            match executor.execute(physical) {
                Ok(rows) => {
                    println!("{} rows", rows.len());
                    for row in rows {
                        println!("{:?}", row);
                    }
                }
                Err(e) => anyhow::bail!("Query error: {}", e),
            }
        }
        
        "filter" => {
            if parts.len() < 5 {
                anyhow::bail!("Usage: filter <table> <column> <op> <value>");
            }
            
            let table = parts[1];
            let column = parts[2];
            let op_str = parts[3];
            let value_str = parts[4];
            
            let op = match op_str {
                "=" | "==" => BinaryOperator::Eq,
                "!=" => BinaryOperator::Ne,
                "<" => BinaryOperator::Lt,
                "<=" => BinaryOperator::Le,
                ">" => BinaryOperator::Gt,
                ">=" => BinaryOperator::Ge,
                _ => anyhow::bail!("Unknown operator: {}", op_str),
            };
            
            let value = if let Ok(i) = value_str.parse::<i64>() {
                Value::Int(i)
            } else {
                Value::String(value_str.to_string())
            };
            
            let filter = Expr::BinaryOp {
                op,
                left: Box::new(Expr::Column(column.to_string())),
                right: Box::new(Expr::Literal(value)),
            };
            
            let logical = planner.plan(table.to_string(), Some(filter));
            let physical = planner.to_physical(logical);
            
            match executor.execute(physical) {
                Ok(rows) => {
                    println!("{} rows", rows.len());
                    for row in rows {
                        println!("{:?}", row);
                    }
                }
                Err(e) => anyhow::bail!("Query error: {}", e),
            }
        }
        
        _ => {
            anyhow::bail!("Unknown command: {}", parts[0]);
        }
    }
    
    Ok(())
}
