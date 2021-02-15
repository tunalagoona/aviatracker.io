import os

from psycopg2 import connect

from config.parser import ConfigParser


def fill_airports_table() -> None:
    script_dir = os.path.abspath(__file__ + "/../../../")
    rel_path = "extra/airports.rtf"
    path = os.path.join(script_dir, rel_path)
    with open(path, "rt") as f:
        lines = f.readlines()
        values = []
        for line in lines:
            values.append(line.split(","))

    conf = ConfigParser()
    conn = connect(dbname=conf.dbname, user=conf.user_name, password=conf.password, host=conf.hostname,
                   port=conf.port_number)
    with conn:
        with conn.cursor() as curs:
            for value in values:
                row = {
                    "airport_id": value[0],
                    "name": value[1],
                    "city": value[2],
                    "country": value[3],
                    "iata": value[4],
                    "icao": value[5],
                    "latitude": value[6],
                    "longitude": value[7],
                    "altitude": value[8],
                    "timezone": value[9],
                    "dst": value[10],
                    "tz_database_time_zone": value[11],
                    "item_type": value[12],
                    "source": value[13]
                }
                curs.execute(
                    """
                        INSERT INTO airports (airport_id, name, city, country, iata, icao, latitude, longitude, altitude, 
                        timezone, dst, tz_database_time_zone, type, source)
                        VALUES (%(airport_id)s, %(name)s, %(city)s, %(country)s, %(iata)s, %(icao)s, %(latitude)s, %(longitude)s, 
                        %(altitude)s, %(timezone)s, %(dst)s, %(tz_database_time_zone)s, %(item_type)s, %(source)s)
                    """, row)


if __name__ == "__main__":
    fill_airports_table()



