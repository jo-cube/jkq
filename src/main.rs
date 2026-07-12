mod cli;
#[allow(dead_code)]
mod output;
#[allow(dead_code)]
mod transform;

use clap::Parser;

fn main() {
    match cli::RawCli::parse().resolve() {
        Err(error) => {
            eprintln!("jkq: {error}");
            std::process::exit(2);
        }
        Ok(_) => {
            eprintln!("jkq: Kafka consumption is not implemented in this initial build");
            std::process::exit(1);
        }
    }
}
