use clap::Parser;
use log::info;
use serde::Deserialize;

pub const CSV_URL: &str =
    "https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv";

#[derive(Deserialize, Debug)]
struct Row {
    #[serde(rename(deserialize = "Location"))]
    location: String,
    #[serde(rename(deserialize = "Url"))]
    url: String,
}

#[derive(Parser, Debug)]
#[command(name = "msfp2pgsql")]
#[command(author = "Jorge Martinez <jorge.martinezgomez@wfp.org>")]
#[command(version = "0.1")]
#[command(about = "Ingest data into postgresql of microsoft building footprint", long_about = None)]
struct Cli {
    #[arg(long, short)]
    list: bool,
}

fn list_countries() -> Vec<String> {
    let csv_text = reqwest::blocking::get(CSV_URL)
        .expect("Could not execute get request")
        .text()
        .expect("Could not transform to text");

    let mut reader = csv::Reader::from_reader(csv_text.as_bytes());
    let iter = reader.deserialize();

    let mut country_set: Vec<String> = Vec::new();
    iter.map(|row: Result<Row, csv::Error>| row.unwrap().location)
        .for_each(|location_name| {
            if !country_set.contains(&location_name) {
                country_set.push(location_name)
            }
        });

    country_set.sort();

    country_set
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    if cli.list {
        info!("Fetching list of available countries");
        println!(
            "{:?}",
            list_countries()
                .into_iter()
                .for_each(|country_name| println!("- {}", country_name))
        );
    }

    //reader.records().for_each(|row| println!("{:?}", row));

    //println!("{:?}", resp);
}
