mod acled;
mod cli;
mod config;

use clap::Parser;
use cli::Cli;

pub const GEOMETRY_FIELD: &str = "geom";

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = Cli::parse();

    match args {
        Cli::Acled(args) => acled::process_command(args),
        _ => panic!("AAAAA"),
    }
}
