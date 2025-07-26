use clap::Parser;
use core::run_query;
/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// SQL query to run
    #[arg(short, long)]
    sql: String,
}

fn main() {
    let args = Args::parse();

    let query = parse(args.sql).unwrap();
    let result = run_query(query);

    println!("{}", result)
}
