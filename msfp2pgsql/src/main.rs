use clap::Parser;
use flate2::read::GzDecoder;
use log::info;
use serde::Deserialize;
use serde_json::Value;
use std::io::prelude::*;

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
    #[arg(long, short)]
    name: String,
}

fn get_urls() -> Vec<Row> {
    let csv_text = reqwest::blocking::get(CSV_URL)
        .expect("Could not execute get request")
        .text()
        .expect("Could not transform to text");

    let mut reader = csv::Reader::from_reader(csv_text.as_bytes());
    let iter = reader.deserialize();

    let rows: Vec<Row> = iter
        .map(|row: Result<Row, csv::Error>| row.unwrap())
        .collect();

    rows
}

fn list_countries(rows: Vec<Row>) -> Vec<String> {
    let locations: Vec<String> = rows.into_iter().map(|row: Row| row.location).collect();

    let mut country_set: Vec<String> = Vec::new();

    locations.into_iter().for_each(|location| {
        if !country_set.contains(&location) {
            country_set.push(location)
        }
    });

    country_set.sort();

    country_set
}

fn get_geometries_from_url(url: String) -> Vec<Value> {
    let gzip_bytes = reqwest::blocking::get(url)
        .expect("Failed retreiving data from url")
        .bytes()
        .expect("Could not deserialize as bytes");

    let mut data_gz = GzDecoder::new(gzip_bytes.as_ref());
    let mut data_str = String::new();
    data_gz.read_to_string(&mut data_str).unwrap();

    let items = data_str
        .split("\n")
        .filter(|item| item != &"")
        .map(|item| {
            serde_json::from_str(item).expect(&format!("Failing deserializing json {}", item))
        })
        .map(|feature: Value| {
            feature
                .get("geometry")
                .expect("Missing geometry field")
                .to_owned()
        })
        .collect::<Vec<Value>>();

    items
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    let rows = get_urls();

    if cli.list {
        info!("Fetching list of available countries");
        println!(
            "{:?}",
            list_countries(rows)
                .into_iter()
                .for_each(|country_name| println!("- {}", country_name))
        );

        return;
    }

    // Filter urls per location name.
    let location_urls = rows
        .into_iter()
        .filter(|row| row.location == cli.name)
        .map(|row| row.url)
        .collect::<Vec<String>>();

    let url: String = location_urls[0].clone();

    let items = get_geometries_from_url(url);

    println!("{:?}", items);
}
