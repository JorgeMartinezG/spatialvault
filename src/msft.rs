use crate::cli::MsftArgs;
use flate2::read::GzDecoder;
use log::{error, info};
use postgres::{Client, NoTls};
use serde::Deserialize;
use serde_json::Value;
use std::io::prelude::*;

const CSV_URL: &str =
    "https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv";

const CHUNK_SIZE: usize = 2000;

#[derive(Deserialize, Debug)]
struct Row {
    #[serde(rename(deserialize = "Location"))]
    location: String,
    #[serde(rename(deserialize = "Url"))]
    url: String,
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

fn list_countries(rows: Vec<Row>) -> () {
    info!("Fetching list of available countries");

    let locations: Vec<String> = rows.into_iter().map(|row: Row| row.location).collect();

    let mut country_set: Vec<String> = Vec::new();

    locations.into_iter().for_each(|location| {
        if !country_set.contains(&location) {
            country_set.push(location)
        }
    });

    country_set.sort();

    country_set
        .into_iter()
        .for_each(|country_name| println!("- {}", country_name));
}

fn get_features_from_url(url: String) -> Vec<Value> {
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

fn create_postgres_table(args: &MsftArgs) {
    let db_url = match &args.db_url {
        Some(value) => value,
        None => {
            error!("Missing parameter database-url");
            std::process::exit(1)
        }
    };

    info!("Creating postgresql table!");

    let mut client = match Client::connect(db_url, NoTls) {
        Ok(client) => client,
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    };

    let query = format!("CREATE SCHEMA IF NOT EXISTS {}", args.db_schema);

    match client.execute(&query, &[]) {
        Ok(_num) => (),
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    }

    let query = format!(
        r#"

            CREATE TABLE {}.{} (
                id SERIAL PRIMARY KEY,
                geom GEOMETRY(POLYGON, 4326) NOT NULL
            )

            "#,
        args.db_schema, args.table_name
    );
    match client.execute(&query, &[]) {
        Ok(_num) => (),
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    }
}

fn into_sql(feature: Value) -> String {
    let coordinates = &feature
        .get("coordinates")
        .expect("coordinates field not found")[0]
        .as_array()
        .expect("Could not transform array of coordinates to rust type");

    let coordinates_arr = &coordinates
        .into_iter()
        .map(|item| {
            item.as_array()
                .expect("Could not transform")
                .iter()
                .map(|latlng| latlng.as_f64().unwrap().to_string())
                .collect::<Vec<String>>()
                .join(" ")
        })
        .collect::<Vec<String>>()
        .join(",");
    let st_geom = format!("ST_GEOMFROMTEXT('POLYGON(({coordinates_arr}))', 4326)");

    st_geom
}

fn process_url(url: String, client: &mut Client, args: &MsftArgs) {
    info!("Processing url {url}");

    let features = get_features_from_url(url);

    let st_geoms = features
        .into_iter()
        .map(|feature| into_sql(feature))
        .collect::<Vec<String>>();

    // Insert geometries into database.
    st_geoms.chunks(CHUNK_SIZE).for_each(|chunk| {
        let st_geoms_query = chunk
            .iter()
            .map(|st_geom| format!("({})", st_geom))
            .collect::<Vec<String>>()
            .join(",");

        let query = format!(
            "INSERT INTO {}.{}(geom) VALUES {}",
            args.db_schema, args.table_name, st_geoms_query
        );

        match client.execute(&query, &[]) {
            Ok(_num) => (),
            Err(err) => {
                error!("{}", err);
                std::process::exit(1)
            }
        }
    })
}

pub fn process_command(args: MsftArgs) {
    if args.list {
        let rows = get_urls();
        list_countries(rows);
        return;
    }

    let db_url: String = match &args.db_url {
        Some(value) => value.to_string(),
        None => {
            error!("Missing parameter database-url");
            std::process::exit(1)
        }
    };

    if args.create_table {
        create_postgres_table(&args);
        return;
    }

    let rows = get_urls();

    let location_urls = rows
        .into_iter()
        .filter(|row| {
            row.location
                == args
                    .name
                    .clone()
                    .unwrap_or_else(|| panic!("name parameter missing"))
        })
        .map(|row| row.url)
        .collect::<Vec<String>>();

    let mut client = match Client::connect(&db_url, NoTls) {
        Ok(client) => client,
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    };

    location_urls
        .into_iter()
        .for_each(|url| process_url(url, &mut client, &args));
}
