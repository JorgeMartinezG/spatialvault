use crate::cli::AcledArgs;
use crate::config::{AcledApi, AcledConfig, AcledPg, PgUrl};
use crate::GEOMETRY_FIELD;
use serde::ser::{Serialize, SerializeStruct, Serializer};
use std::collections::HashMap;

use log::{error, info};
use postgres::{Client, NoTls};
use reqwest::blocking::Client as RClient;
use std::rc::Rc;

use serde_json::{Map, Value};

enum PgType {
    Varchar,
    Float,
    Int,
    Point,
}

pub struct Request {
    api_config: Rc<AcledConfig>,
    page: u8,
    iso: i16,
}

impl Request {
    pub fn new(api_config: Rc<AcledConfig>, page: u8, iso: i16) -> Self {
        Self {
            api_config,
            page,
            iso,
        }
    }
}

impl Serialize for Request {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let AcledApi {
            start_date,
            end_date,
            key,
            email,
            ..
        } = &self.api_config.api;

        let event_date = format!("{start_date}|{end_date}");

        let mut s = serializer.serialize_struct("Request", 6)?;
        s.serialize_field("key", &key)?;
        s.serialize_field("email", &email)?;
        s.serialize_field("page", &self.page)?;
        s.serialize_field("iso", &self.iso)?;
        s.serialize_field("event_date", &event_date)?;
        s.serialize_field("event_date_where", "BETWEEN")?;
        s.end()
    }
}

fn create_query_string(acled_config: &AcledConfig, table_fields: &HashMap<&str, PgType>) -> String {
    // Getting keys elements
    let keys = table_fields
        .keys()
        .map(|k| k.clone())
        .collect::<Vec<&str>>();
    let keys_str = keys.join(",");
    /*
    let indicators: Vec<String> = keys
        .iter()
        .enumerate()
        .map(|(i, _j)| {
            let position = i + 1;
            format!("${position}")
        })
        .collect();
    let indicators_str = indicators.join(",");
    */
    let AcledPg { schema, table_name } = &acled_config.pg;
    let query = format!("INSERT INTO {}.{}({keys_str})", schema, table_name);

    query
}

fn create_pg_table(acled_config: &AcledConfig, pg: &PgUrl, table_fields: &HashMap<&str, PgType>) {
    info!("Creating postgresql table!");
    let mut client = match Client::connect(&pg.0, NoTls) {
        Ok(client) => client,
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    };

    let AcledPg { schema, table_name } = &acled_config.pg;

    let query = format!("CREATE SCHEMA IF NOT EXISTS {}", schema);

    match client.execute(&query, &[]) {
        Ok(_num) => (),
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    }

    let fields_str = table_fields
        .iter()
        .map(|(key, field_type)| {
            let field_str = match field_type {
                PgType::Varchar => "VARCHAR NOT NULL",
                PgType::Int => "BIGINT NOT NULL",
                PgType::Float => "REAL NOT NULL",
                PgType::Point => "GEOMETRY(POINT, 4326) NOT NULL",
            };

            format!("{key} {field_str}")
        })
        .collect::<Vec<String>>()
        .join(",\n");

    let query = format!(
        r#"
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({fields_str});
        "#,
    );
    match client.execute(&query, &[]) {
        Ok(_num) => (),
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    }
}

fn add_values_to_query(
    query_str: &str,
    item: &Map<String, Value>,
    table_fields: &HashMap<&str, PgType>,
) -> String {
    let lat = item
        .get("latitude")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<f64>().ok())
        .expect("Invalid latitude value");
    let lon = item
        .get("longitude")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<f64>().ok())
        .expect("Invalid latitude value");

    let geom = format!("ST_GEOMFROMTEXT('POINT ({lat} {lon})', 4326)");

    let parsed_values = table_fields
        .iter()
        .map(|(field_key, field_type)| {
            if *field_key == GEOMETRY_FIELD {
                return geom.clone();
            }

            let value = item
                .get(*field_key)
                .and_then(|v| v.as_str())
                .map(|v| v.to_string().replace("'", "''"))
                .expect("Invalid value");

            match field_type {
                PgType::Varchar => format!("'{value}'"),
                _ => value,
            }
        })
        .collect::<Vec<String>>();

    let values_str = parsed_values.join(",");

    format!("{query_str} VALUES ({values_str})")
}

pub fn process_command(args: AcledArgs) {
    // Load postgres configuration
    let pg_url = PgUrl::new(args.pg_config.to_str().unwrap());
    let acled_config = AcledConfig::new(args.config.to_str().unwrap());

    let table_fields = HashMap::from([
        ("actor1", PgType::Varchar),
        ("actor2", PgType::Varchar),
        ("admin1", PgType::Varchar),
        ("admin2", PgType::Varchar),
        ("admin3", PgType::Varchar),
        ("assoc_actor_1", PgType::Varchar),
        ("assoc_actor_2", PgType::Varchar),
        ("civilian_targeting", PgType::Varchar),
        ("country", PgType::Varchar),
        ("disorder_type", PgType::Varchar),
        ("event_date", PgType::Varchar),
        ("event_id_cnty", PgType::Varchar),
        ("event_type", PgType::Varchar),
        ("fatalities", PgType::Int),
        ("geo_precision", PgType::Int),
        ("inter1", PgType::Varchar),
        ("inter2", PgType::Varchar),
        ("interaction", PgType::Varchar),
        ("iso", PgType::Int),
        ("latitude", PgType::Float),
        ("location", PgType::Varchar),
        ("longitude", PgType::Float),
        ("notes", PgType::Varchar),
        ("region", PgType::Varchar),
        ("source", PgType::Varchar),
        ("source_scale", PgType::Varchar),
        ("sub_event_type", PgType::Varchar),
        ("tags", PgType::Varchar),
        ("time_precision", PgType::Varchar),
        ("timestamp", PgType::Varchar),
        ("year", PgType::Varchar),
        (GEOMETRY_FIELD, PgType::Point),
    ]);

    create_pg_table(&acled_config, &pg_url, &table_fields);

    let rc_config = Rc::new(acled_config.clone());
    let client = RClient::new();

    let mut pg_client = match Client::connect(&pg_url.0, NoTls) {
        Ok(client) => client,
        Err(err) => {
            error!("{}", err);
            std::process::exit(1)
        }
    };

    let query_str = create_query_string(&acled_config, &table_fields);

    rc_config.codes.iter().for_each(|(iso, code)| {
        info!("Fetching data for country {:?}", iso);
        let mut page = 1;

        loop {
            let request = Request::new(rc_config.clone(), page, *code);

            let resp: Value = client
                .get(&rc_config.api.url)
                .query(&request)
                .send()
                .and_then(|resp| resp.json())
                .expect("Failed performing Acled request");

            let count = resp
                .get("count")
                .and_then(|v| v.as_u64())
                .expect("Invalid count field");

            if count == 0 {
                break;
            }

            let items: Vec<&Map<String, Value>> = resp
                .as_object()
                .and_then(|o| o.get("data"))
                .and_then(|o| o.as_array())
                .expect("Array values not found")
                .into_iter()
                .map(|item: &Value| item.as_object().unwrap())
                .collect();

            let query_values = items
                .into_iter()
                .map(|item| add_values_to_query(&query_str, item, &table_fields))
                .collect::<Vec<String>>();

            query_values
                .iter()
                .for_each(|query| match pg_client.execute(query, &[]) {
                    Ok(_num) => (),
                    Err(err) => {
                        error!("{}", err);
                        std::process::exit(1)
                    }
                });

            page += 1;
        }
    });

    //println!("{:?}", acled_config);
}
