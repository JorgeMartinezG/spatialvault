use crate::acled::Params;

use std::collections::HashMap;
use std::fs::read_to_string;

use serde::{Deserialize, Deserializer};

use toml::Value;

#[derive(Debug)]
pub struct Config {
    pub acled: Params,
    pub db_url: String,
    pub country_codes: HashMap<String, i16>,
}

impl Config {
    pub fn new(config_file: &str) -> Self {
        let content = read_to_string(config_file).unwrap();

        let config: Config = toml::from_str(content.as_str()).expect("Cannot read config file");

        config
    }
}

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        let acled: Params = Value::try_into(
            value
                .get("acled")
                .expect("Could not get acled field")
                .to_owned(),
        )
        .expect("could not deserialize acled field");

        // Configuring database parameters.
        let db = value.get("database").expect("Missing database section");

        let host = db
            .get("host")
            .expect("Missing parameter host")
            .as_str()
            .expect("Parameter host is not a string");
        let user = db
            .get("user")
            .expect("Missing parameter user")
            .as_str()
            .expect("Parameter user is not a string");
        let password = db
            .get("password")
            .expect("Missing parameter password")
            .as_str()
            .expect("Parameter password is not a string");
        let port = db
            .get("port")
            .expect("Missing parameter port")
            .as_integer()
            .expect("Parameter port is not a number");
        let name = db
            .get("name")
            .expect("Missing parameter name")
            .as_str()
            .expect("Parameter name is not a string");

        let db_url = format!("postgres://{user}:{password}@{host}:{port}/{name}");

        let country_codes: HashMap<String, i16> = Value::try_into(
            value
                .get("country_codes")
                .expect("could not get country_codes field")
                .to_owned(),
        )
        .expect("could not deserialize country_codes");

        let config = Config {
            db_url,
            acled,
            country_codes: country_codes,
        };

        Ok(config)
    }
}
