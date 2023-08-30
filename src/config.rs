use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::fs::read_to_string;
use toml::Value;

fn default_schema() -> String {
    "public".into()
}

fn date_today() -> NaiveDate {
    Utc::now().date_naive()
}

#[derive(Debug)]
pub struct PgUrl(pub String);

impl PgUrl {
    pub fn new(config_file: &str) -> Self {
        let content = read_to_string(config_file).unwrap();
        let config: Self = toml::from_str(content.as_str()).expect("Cannot read config file");
        config
    }
}

impl<'de> Deserialize<'de> for PgUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let connection_params = Value::deserialize(deserializer)?;

        let host = connection_params
            .get("host")
            .expect("Missing parameter host")
            .as_str()
            .expect("Parameter host is not a string");
        let user = connection_params
            .get("user")
            .expect("Missing parameter user")
            .as_str()
            .expect("Parameter user is not a string");
        let password = connection_params
            .get("password")
            .expect("Missing parameter password")
            .as_str()
            .expect("Parameter password is not a string");
        let port = connection_params
            .get("port")
            .expect("Missing parameter port")
            .as_integer()
            .expect("Parameter port is not a number");
        let name = connection_params
            .get("name")
            .expect("Missing parameter name")
            .as_str()
            .expect("Parameter name is not a string");

        let db_url = format!("postgres://{user}:{password}@{host}:{port}/{name}");

        Ok(PgUrl(db_url))
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct AcledPg {
    pub table_name: String,
    #[serde(default = "default_schema")]
    pub schema: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AcledApi {
    pub url: String,
    pub key: String,
    pub email: String,
    pub start_date: NaiveDate,
    #[serde(default = "date_today")]
    pub end_date: NaiveDate,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AcledConfig {
    pub pg: AcledPg,
    pub api: AcledApi,
    pub codes: HashMap<String, i16>,
}

impl AcledConfig {
    pub fn new(config_file: &str) -> Self {
        let content = read_to_string(config_file).unwrap();
        let config: Self = toml::from_str(content.as_str()).expect("Cannot read config file");
        config
    }
}
