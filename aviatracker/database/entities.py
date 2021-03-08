from collections import namedtuple

StateVector = namedtuple(
    "StateVector",
    [
        "request_time",
        "icao24",
        "callsign",
        "origin_country",
        "time_position",
        "last_contact",
        "longitude",
        "latitude",
        "baro_altitude",
        "on_ground",
        "velocity",
        "true_track",
        "vertical_rate",
        "sensors",
        "geo_altitude",
        "squawk",
        "spi",
        "position_source",
    ],
)

FlightPath = namedtuple(
    "FlightPath",
    [
        "last_update",
        "icao24",
        "callsign",
        "departure_airport_icao",
        "arrival_airport_icao",
        "path",
        "finished",
        "finished_at"
    ],
)

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

FlightAirportInfo = namedtuple(
    "FlightAirportInfo",
    [
        "icao24",
        "estDepartureAirport",
        "estArrivalAirport",
        "estArrivalTime"
    ],
)

AirportStats = namedtuple(
    "AirportStats",
    [
        "airport_icao",
        "the_date",
        "airplane_quantity_arrivals",
        "airplane_quantity_departures"
    ]
)

CallsignMemo = namedtuple(
    "CallsignMemo",
    [
        "callsign",
        "arrival_airport",
        "departure_airport"
    ]
)

OpenskyFlight = namedtuple(
    "OpenskyFlight",
    [
        "icao24",
        "firstSeen",
        "estDepartureAirport",
        "lastSeen",
        "estArrivalAirport",
        "callsign",
        "estDepartureAirportHorizDistance",
        "estDepartureAirportVertDistance",
        "estArrivalAirportHorizDistance",
        "estArrivalAirportVertDistance",
        "departureAirportCandidatesCount",
        "arrivalAirportCandidatesCount"
    ]
)
