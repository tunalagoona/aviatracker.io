from collections import namedtuple

Airport = namedtuple(
    "Airport",
    [
        "airport_id",
        "name",
        "city",
        "iata",
        "icao",
        "latitude",
        "longitude",
        "altitude",
        "timezone",
        "dst",
        "tz_database_time_zone",
        "item_type",
        "source",
    ],
)
