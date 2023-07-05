// @generated automatically by Diesel CLI.

pub mod acled {
    pub mod sql_types {
        #[derive(diesel::sql_types::SqlType)]
        #[diesel(postgres_type(name = "geometry"))]
        pub struct Geometry;
    }

    diesel::table! {
        use diesel::sql_types::*;
        use super::sql_types::Geometry;

        wfp.wld_inc_acled (event_id_cnty) {
            event_id_cnty -> Varchar,
            actor1 -> Varchar,
            actor2 -> Varchar,
            assoc_actor_1 -> Varchar,
            assoc_actor_2 -> Varchar,
            civilian_targeting -> Varchar,
            disorder_type -> Varchar,
            event_date -> Date,
            event_type -> Varchar,
            fatalities -> Int8,
            iso -> Int2,
            notes -> Varchar,
            source -> Varchar,
            source_scale -> Varchar,
            sub_event_type -> Varchar,
            timestamp -> Int8,
            year -> Int4,
            iso3 -> Varchar,
            geom -> Geometry,
        }
    }
}
