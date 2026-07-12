mod app;
mod cli;
mod kafka;
mod output;
mod runtime;
mod transform;

use clap::Parser;

fn main() {
    match cli::RawCli::parse().resolve() {
        Err(error) => {
            eprintln!("jkq: {error}");
            std::process::exit(2);
        }
        Ok(config) => {
            if let Err(error) = app::run(config) {
                if error.is_broken_pipe() {
                    return;
                }
                eprintln!("jkq: {error}");
                std::process::exit(error.exit_code());
            }
        }
    }
}
