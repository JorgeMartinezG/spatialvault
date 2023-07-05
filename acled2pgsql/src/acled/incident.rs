use crate::schema::acled::sql_types::Geometry;
use crate::schema::acled::wld_inc_acled;
use chrono::NaiveDate;
use postgis::ewkb::{AsEwkbPoint, EwkbRead, EwkbWrite, GeometryT, Point};
use std::io::Cursor;

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use diesel::{
    deserialize::{self, FromSql},
    pg::{self, Pg},
    prelude::*,
    serialize::{self, IsNull, Output, ToSql},
    AsExpression, FromSqlRow,
};

#[derive(Debug, FromSqlRow, AsExpression)]
#[diesel(sql_type = Geometry)]
pub struct PointType(pub Point);

impl ToSql<Geometry, Pg> for PointType {
    fn to_sql(&self, out: &mut Output<Pg>) -> serialize::Result {
        self.0.as_ewkb().write_ewkb(out)?;
        Ok(IsNull::No)
    }
}

impl FromSql<Geometry, Pg> for PointType {
    fn from_sql(bytes: pg::PgValue) -> deserialize::Result<Self> {
        let mut r = Cursor::new(bytes.as_bytes());
        let geom = GeometryT::read_ewkb(&mut r)?;
        return match geom {
            postgis::ewkb::GeometryT::Point(point) => Ok(PointType(point)),
            _ => Err("Geometry is not a point".into()),
        };
    }
}

#[derive(Debug, Queryable, Insertable)]
#[diesel(table_name = wld_inc_acled)]
pub struct Incident {
    actor1: String,
    actor2: String,
    assoc_actor_1: String,
    assoc_actor_2: String,
    civilian_targeting: String,
    disorder_type: String,
    event_date: NaiveDate,
    event_type: String,
    event_id_cnty: String,
    fatalities: i64,
    iso: i16,
    notes: String,
    source: String,
    source_scale: String,
    sub_event_type: String,
    timestamp: i64,
    year: i32,
    geom: PointType,
    iso3: Option<String>,
}

impl Incident {
    pub fn with_iso3(self, iso3: &str) -> Self {
        Self {
            iso3: Some(iso3.to_string()),
            ..self
        }
    }
}

impl<'de> Deserialize<'de> for Incident {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let json: Value = Value::deserialize(deserializer)?;
        let latitude = json
            .get("latitude")
            .expect("latitude not found")
            .as_str()
            .expect("latitude is not string")
            .parse::<f64>()
            .expect("Failed parsing latitude");

        let longitude = json
            .get("longitude")
            .expect("longitude not found")
            .as_str()
            .expect("longitude is not string")
            .parse::<f64>()
            .expect("Failed parsing longitude");

        Ok(Self {
            actor1: json
                .get("actor1")
                .expect("actor1 not found")
                .as_str()
                .expect("actor1 is not string")
                .to_string(),
            actor2: json
                .get("actor2")
                .expect("actor2 not found")
                .as_str()
                .expect("actor2 is not string")
                .to_string(),
            assoc_actor_1: json
                .get("assoc_actor_1")
                .expect("assoc_actor_1 not found")
                .as_str()
                .expect("assoc_actor_1 is not string")
                .to_string(),
            assoc_actor_2: json
                .get("assoc_actor_2")
                .expect("assoc_actor_2 not found")
                .as_str()
                .expect("assoc_actor_2 is not string")
                .to_string(),
            civilian_targeting: json
                .get("civilian_targeting")
                .expect("civilian_targeting not found")
                .as_str()
                .expect("civilian_targeting is not string")
                .to_string(),
            disorder_type: json
                .get("disorder_type")
                .expect("disorder_type not found")
                .as_str()
                .expect("disorder_type is not string")
                .to_string(),
            event_type: json
                .get("event_type")
                .expect("event_type not found")
                .as_str()
                .expect("event_type is not string")
                .to_string(),
            event_id_cnty: json
                .get("event_id_cnty")
                .expect("event_id_cnty not found")
                .as_str()
                .expect("event_id_cnty is not string")
                .to_string(),
            event_date: NaiveDate::parse_from_str(
                json.get("event_date")
                    .expect("event_date not found")
                    .as_str()
                    .expect("event_date is not a string"),
                "%Y-%m-%d",
            )
            .expect("Failed parsing event_date"),
            year: json
                .get("year")
                .expect("year not found")
                .as_str()
                .expect("year is not string")
                .parse::<i32>()
                .expect("Failed parsing year"),
            fatalities: json
                .get("fatalities")
                .expect("fatalities not found")
                .as_str()
                .expect("fatalities is not string")
                .parse::<i64>()
                .expect("Failed parsing fatalities"),
            iso: json
                .get("iso")
                .expect("iso not found")
                .as_str()
                .expect("iso is not string")
                .parse::<i16>()
                .expect("Failed parsing iso"),
            notes: json
                .get("notes")
                .expect("notes not found")
                .as_str()
                .expect("notes is not string")
                .to_string(),
            source: json
                .get("source")
                .expect("source not found")
                .as_str()
                .expect("source is not string")
                .to_string(),
            source_scale: json
                .get("source_scale")
                .expect("source_scale not found")
                .as_str()
                .expect("source_scale is not string")
                .to_string(),
            sub_event_type: json
                .get("sub_event_type")
                .expect("sub_event_type not found")
                .as_str()
                .expect("sub_event_type is not string")
                .to_string(),
            timestamp: json
                .get("timestamp")
                .expect("timestamp not found")
                .as_str()
                .expect("timestamp is not string")
                .parse::<i64>()
                .expect("Failed parsing timestamp"),
            //geom: Point::new(longitude, latitude, Some(4326)),
            geom: PointType(Point::new(longitude, latitude, Some(4326))),
            iso3: None,
        })
    }
}
