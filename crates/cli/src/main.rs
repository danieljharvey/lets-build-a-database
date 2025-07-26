use clap::Parser;
use core::{parse, run_query};
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

    let query = parse(&args.sql).unwrap();
    let results = run_query(&query);

    for result in results {
        println!("{result}");
    }
}
