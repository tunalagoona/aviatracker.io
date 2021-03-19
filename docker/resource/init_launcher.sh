python3 -m aviatracker.scripts.init_database.py make-tables
python3 -m aviatracker.scripts.init_database.py make-tables fill-callsigns
python3 -m aviatracker.scripts.init_database.py make-tables fill-airports --file="/extra/airports.txt"
