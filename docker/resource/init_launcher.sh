python3 -m aviatracker.scripts.init_database make-tables
python3 -m aviatracker.scripts.init_database fill-callsigns
python3 -m aviatracker.scripts.init_database fill-airports --file="/extra/airports.txt"
