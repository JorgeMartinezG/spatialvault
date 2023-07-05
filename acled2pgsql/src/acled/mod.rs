pub mod incident;

use chrono::{NaiveDate, Utc};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde::Deserialize;
use std::rc::Rc;

use crate::acled::incident::Incident;
use crate::config::Config;

#[derive(Deserialize, Debug)]
pub struct Response {
    pub status: u8,
    pub success: bool,
    pub last_update: i32,
    pub count: u32,
    pub data: Vec<Incident>,
    pub filename: String,
}

#[derive(Debug, Deserialize)]
pub struct Params {
    pub api_url: String,
    key: String,
    email: String,
    start_date: Option<NaiveDate>,
}

pub struct Request {
    config: Rc<Config>,
    page: u8,
    iso: i16,
}

impl Request {
    pub fn new(config: Rc<Config>, page: u8, iso: i16) -> Self {
        Self { config, page, iso }
    }
}

impl Serialize for Request {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Params {
            start_date,
            key,
            email,
            ..
        } = &self.config.acled;
        let end_date = Utc::now().date_naive();
        let start = start_date.unwrap();
        let event_date = format!("{start}|{end_date}");

        let mut s = serializer.serialize_struct("Request", 6)?;
        s.serialize_field("key", key)?;
        s.serialize_field("email", email)?;
        s.serialize_field("page", &self.page)?;
        s.serialize_field("iso", &self.iso)?;
        s.serialize_field("event_date", &event_date)?;
        s.serialize_field("event_date_where", "BETWEEN")?;
        s.end()
    }
}
