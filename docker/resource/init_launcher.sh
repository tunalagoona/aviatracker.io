python3 -m aviatracker.scripts.init_database make-tables
python3 -m aviatracker.scripts.init_database make-tables fill-callsigns
python3 -m aviatracker.scripts.init_database make-tables fill-airports --file="/extra/airports.txt"
