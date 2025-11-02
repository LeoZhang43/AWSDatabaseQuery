#!/usr/bin/env python3
import argparse
import os
import psycopg2
import csv
from datetime import datetime

def run_schema(cursor, schema_file):
    """Run schema.sql to create tables"""
    with open(schema_file, "r", encoding="utf-8") as f:
        sql = f.read()
    cursor.execute(sql)

def parse_datetime(val):
    """Convert CSV datetime string to Python datetime"""
    if val is None or val == "":
        return None
    return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")

def load_csv(cursor, table, csv_file, column_map=None, unique_columns=None, convert=None):
    """
    Load CSV into table.
    - column_map: dict of csv_column -> table_column
    - unique_columns: list of columns to use ON CONFLICT DO NOTHING
    - convert: dict of column -> conversion function
    """
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        columns = row.keys() if not column_map else column_map.keys()
        table_columns = columns if not column_map else [column_map[c] for c in columns]
        values = []
        for c in columns:
            val = row[c]
            if convert and c in convert:
                val = convert[c](val)
            values.append(val)

        placeholders = ",".join(["%s"] * len(values))

        if unique_columns:
            conflict_cols = ",".join(unique_columns)
            sql = f"""
            INSERT INTO {table} ({','.join(table_columns)})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols}) DO NOTHING
            """
        else:
            sql = f"INSERT INTO {table} ({','.join(table_columns)}) VALUES ({placeholders})"

        cursor.execute(sql, values)

    return len(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--dbname", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--datadir", required=True)
    parser.add_argument("--schema", default="schema.sql")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.host,
        dbname=args.dbname,
        user=args.user,
        password=args.password
    )
    conn.autocommit = True
    cursor = conn.cursor()

    print(f"Connected to {args.dbname}@{args.host}")
    print("Creating schema...")
    run_schema(cursor, args.schema)
    print("Tables created: lines, stops, line_stops, trips, stop_events\n")

    total = 0

    # Load lines
    lines_file = os.path.join(args.datadir, "lines.csv")
    n = load_csv(cursor, "lines", lines_file, unique_columns=["line_name"])
    print(f"Loading {lines_file}... {n} rows")
    total += n

    # Load stops
    stops_file = os.path.join(args.datadir, "stops.csv")
    n = load_csv(cursor, "stops", stops_file, unique_columns=["stop_name"])
    print(f"Loading {stops_file}... {n} rows")
    total += n

    # Resolve foreign keys
    cursor.execute("SELECT line_name, line_id FROM lines")
    line_map = dict(cursor.fetchall())
    cursor.execute("SELECT stop_name, stop_id FROM stops")
    stop_map = dict(cursor.fetchall())

    # Load line_stops
    line_stops_file = os.path.join(args.datadir, "line_stops.csv")
    with open(line_stops_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for row in rows:
        cursor.execute(
            "INSERT INTO line_stops (line_id, stop_id, sequence_number, time_offset_minutes) VALUES (%s,%s,%s,%s) ON CONFLICT (line_id, stop_id) DO NOTHING",
            (line_map[row["line_name"]], stop_map[row["stop_name"]], int(row["sequence"]), int(row["time_offset"]))
        )
    print(f"Loading {line_stops_file}... {len(rows)} rows")
    total += len(rows)

    # Load trips
    trips_file = os.path.join(args.datadir, "trips.csv")
    with open(trips_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for row in rows:
        cursor.execute(
            "INSERT INTO trips (trip_id, line_id, departure_time, vehicle_id) VALUES (%s,%s,%s,%s) ON CONFLICT (line_id, departure_time, vehicle_id) DO NOTHING",
            (row["trip_id"], line_map[row["line_name"]], parse_datetime(row["scheduled_departure"]), row["vehicle_id"])
        )
    print(f"Loading {trips_file}... {len(rows)} rows")
    total += len(rows)

    # Load stop_events
    stop_events_file = os.path.join(args.datadir, "stop_events.csv")
    with open(stop_events_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for row in rows:
        cursor.execute(
            """
            INSERT INTO stop_events
            (trip_id, stop_id, scheduled_time, actual_time, passengers_on, passengers_off)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (trip_id, stop_id) DO NOTHING
            """,
            (row["trip_id"], stop_map[row["stop_name"]],
             parse_datetime(row["scheduled"]), parse_datetime(row["actual"]),
             int(row["passengers_on"]), int(row["passengers_off"]))
        )
    print(f"Loading {stop_events_file}... {len(rows)} rows")
    total += len(rows)

    print(f"\nTotal: {total} rows loaded")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
