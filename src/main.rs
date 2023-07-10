mod cli;
mod msft;

use clap::Parser;
use cli::Cli;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = Cli::parse();

    match args {
        Cli::Msft(args) => msft::process_command(args),
    }
}
