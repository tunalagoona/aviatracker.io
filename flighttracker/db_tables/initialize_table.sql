CREATE TABLE opensky_state_vectors (
    request_time INTEGER,
    icao24 VARCHAR,
    callsign VARCHAR,
    origin_country VARCHAR,
    time_position INTEGER,
    last_contact INTEGER,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    baro_altitude DOUBLE PRECISION,
    on_ground BOOLEAN,
    velocity DOUBLE PRECISION,
    true_track DOUBLE PRECISION,
    vertical_rate DOUBLE PRECISION,
    sensors INTEGER,
    geo_altitude DOUBLE PRECISION,
    squawk TEXT,
    spi BOOLEAN,
    position_source INTEGER,
    PRIMARY KEY (request_time, icao24)
);



