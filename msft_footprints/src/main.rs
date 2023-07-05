use clap::Parser;

pub const CSV_URL: &str =
    "https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv";

#[derive(Parser, Debug)]
#[command(name = "acled2pgsql")]
#[command(author = "Jorge Martinez <jorge.martinezgomez@wfp.org>")]
#[command(version = "1.0")]
#[command(about = "Ingest data into postgresql of microsoft building footprint", long_about = None)]
struct Cli {
    #[arg(long)]
    config: String,
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    println!("Hello, world!");
}
