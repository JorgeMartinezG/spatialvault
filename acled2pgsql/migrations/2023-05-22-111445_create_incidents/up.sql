CREATE SCHEMA IF NOT EXISTS wfp;

-- Your SQL goes here
CREATE TABLE wfp.wld_inc_acled (
    event_id_cnty VARCHAR PRIMARY KEY,
    actor1 VARCHAR NOT NULL,
    actor2 VARCHAR NOT NULL,
    assoc_actor_1 VARCHAR NOT NULL,
    assoc_actor_2 VARCHAR NOT NULL,
    civilian_targeting VARCHAR NOT NULL,
    disorder_type VARCHAR NOT NULL,
    event_date DATE NOT NULL,
    event_type VARCHAR NOT NULL,
    fatalities BIGINT NOT NULL,
    iso SMALLINT NOT NULL,
    notes VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    source_scale VARCHAR NOT NULL,
    sub_event_type VARCHAR NOT NULL,
    timestamp BIGINT NOT NULL,
    year INTEGER NOT NULL,
    iso3 VARCHAR NOT NULL,
    geom GEOMETRY(POINT, 4326) NOT NULL
);

CREATE INDEX iso3_idx ON wfp.wld_inc_acled (iso);