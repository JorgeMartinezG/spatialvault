use crate::cli::{MsftArgs, PgParams};
use flate2::read::GzDecoder;
use geo_types::geometry::Polygon;
use geojson::{Feature, GeoJson, Value};
use log::{error, info};
use postgres::{Client, NoTls};
use serde::Deserialize;
use shapefile::record::polygon::Polygon as ShpPolygon;
use std::cell::RefCell;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use wkt::ToWkt;

//use flatgeobuf::{FgbCrs, FgbWriter, FgbWriterOptions, GeometryType};

use shapefile::writer::ShapeWriter;

const CSV_URL: &str =
    "https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv";

const CHUNK_SIZE: usize = 5000;

pub type ShapefileWriter = RefCell<ShapeWriter<BufWriter<File>>>;
pub type PgWriter = (RefCell<Client>, PgParams);

enum OutputType {
    Pg,
    Shp,
    Fgb,
}

enum Output {
    Pg(PgWriter),
    Shp(ShapefileWriter),
}

impl Output {
    fn new(args: &MsftArgs, output_type: OutputType) -> Self {
        match output_type {
            OutputType::Pg => {
                let client = RefCell::new(pg_create_client(&args.pg_params));
                let tuple_data = (client, args.pg_params.clone());
                return Output::Pg(tuple_data);
            }
            OutputType::Shp => {
                let writer = ShapeWriter::from_path("polygons.shp").unwrap();
                Output::Shp(RefCell::new(writer))
            }
            _ => panic!("AAA"),
        }
    }

    pub fn save(&self, rows: Vec<Polygon<f64>>) {
        match self {
            Output::Shp(writer) => {
                rows.into_iter().for_each(|row| {
                    let shp: ShpPolygon = row.try_into().unwrap();
                    writer.borrow_mut().write_shape(&shp).unwrap();
                });
            }
            Output::Pg(params) => pg_save(params, rows),
        }
    }
}

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

fn get_geojson_values(url: String) -> Vec<Value> {
    let gzip_bytes = reqwest::blocking::get(url)
        .expect("Failed retreiving data from url")
        .bytes()
        .expect("Could not deserialize as bytes");

    let mut data_gz = GzDecoder::new(gzip_bytes.as_ref());
    let mut data_str = String::new();
    data_gz.read_to_string(&mut data_str).unwrap();

    data_str
        .split("\n")
        .filter(|item| item != &"")
        .map(|item| {
            let geojson: GeoJson = item.parse::<GeoJson>().unwrap();
            let polygon_value: Value = Feature::try_from(geojson)
                .unwrap()
                .geometry
                .expect("Geometry not found")
                .value;

            return polygon_value;
        })
        .collect()
}

fn pg_save(enum_params: &PgWriter, rows: Vec<Polygon<f64>>) {
    let (client, params) = enum_params;

    let st_geoms: Vec<String> = rows
        .into_iter()
        .map(|row| format!("ST_GEOMFROMTEXT('{}', 4326)", row.wkt_string()))
        .collect();
    // Insert geometries into database.
    st_geoms.chunks(CHUNK_SIZE).for_each(|chunk| {
        let st_geoms_query = chunk
            .iter()
            .map(|st_geom| format!("({})", st_geom))
            .collect::<Vec<String>>()
            .join(",");

        let query = format!(
            "INSERT INTO {}.{}(geom) VALUES {}",
            params.db_schema, params.table_name, st_geoms_query
        );

        match client.borrow_mut().execute(&query, &[]) {
            Ok(_num) => (),
            Err(err) => {
                error!("{}", err);
                std::process::exit(1)
            }
        }
    })
}

fn pg_create_table(args: &MsftArgs) {
    let db_url = match &args.pg_params.db_url {
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

    let query = format!("CREATE SCHEMA IF NOT EXISTS {}", args.pg_params.db_schema);

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
        args.pg_params.db_schema, args.pg_params.table_name
    );
    match client.execute(&query, &[]) {
        Ok(_num) => (),
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    }
}

fn process_url(url: String) -> Vec<Polygon<f64>> {
    info!("Processing url {url}");

    let geojson_polygons = get_geojson_values(url);

    let polygons_geo_types: Vec<Polygon<f64>> = geojson_polygons
        .iter()
        .map(|polygon| Polygon::<f64>::try_from(polygon).unwrap())
        .collect();

    polygons_geo_types
}

fn pg_create_client(pg_params: &PgParams) -> Client {
    let db_url: String = match &pg_params.db_url {
        Some(value) => value.to_owned(),
        None => {
            error!("Missing parameter database-url");
            std::process::exit(1)
        }
    };

    match Client::connect(&db_url, NoTls) {
        Ok(client) => client,
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    }
}

pub fn process_command(args: MsftArgs) {
    if args.list {
        let rows = get_urls();
        list_countries(rows);
        return;
    }

    if args.pg_params.create_table {
        pg_create_table(&args);
        return;
    }

    let output_obj = Output::new(&args, OutputType::Shp);

    let location_urls = get_urls()
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

    location_urls[0..3].into_iter().for_each(|url| {
        let rows = process_url(url.to_string());
        output_obj.save(rows);
    })
}
