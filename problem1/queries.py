#!/usr/bin/env python3
import argparse
import psycopg2
import json
import sys

QUERIES = {
    "Q1": {
        "description": "List all stops on Route 20 in order",
        "sql": """
            SELECT s.stop_name, ls.sequence_number AS sequence, ls.time_offset_minutes AS time_offset
            FROM line_stops ls
            JOIN lines l ON ls.line_id = l.line_id
            JOIN stops s ON ls.stop_id = s.stop_id
            WHERE l.line_name = 'Route 20'
            ORDER BY ls.sequence_number
        """
    },
    "Q2": {
        "description": "Trips during morning rush (7-9 AM)",
        "sql": """
            SELECT t.trip_id, l.line_name, t.departure_time AS scheduled_departure
            FROM trips t
            JOIN lines l ON t.line_id = l.line_id
            WHERE t.departure_time::time >= '07:00:00'::time
              AND t.departure_time::time <= '09:00:00'::time
            ORDER BY t.departure_time
        """
    },
    "Q3": {
        "description": "Transfer stops (stops on 2+ routes)",
        "sql": """
            SELECT s.stop_name, COUNT(DISTINCT ls.line_id) AS line_count
            FROM line_stops ls
            JOIN stops s ON ls.stop_id = s.stop_id
            GROUP BY s.stop_name
            HAVING COUNT(DISTINCT ls.line_id) >= 2
            ORDER BY line_count DESC
        """
    },
    "Q4": {
        "description": "Complete route for trip T0001",
        "sql": """
            SELECT s.stop_name, ls.sequence_number AS sequence, ls.time_offset_minutes AS time_offset
            FROM stop_events se
            JOIN trips t ON se.trip_id = t.trip_id
            JOIN line_stops ls ON t.line_id = ls.line_id AND se.stop_id = ls.stop_id
            JOIN stops s ON se.stop_id = s.stop_id
            WHERE t.trip_id = 'T0001'
            ORDER BY ls.sequence_number
        """
    },
    "Q5": {
        "description": "Routes serving both Wilshire / Veteran and Le Conte / Broxton",
        "sql": """
            SELECT l.line_name
            FROM lines l
            JOIN line_stops ls1 ON l.line_id = ls1.line_id
            JOIN stops s1 ON ls1.stop_id = s1.stop_id
            JOIN line_stops ls2 ON l.line_id = ls2.line_id
            JOIN stops s2 ON ls2.stop_id = s2.stop_id
            WHERE s1.stop_name = 'Wilshire / Veteran'
              AND s2.stop_name = 'Le Conte / Broxton'
        """
    },
    "Q6": {
        "description": "Average ridership by line",
        "sql": """
            SELECT l.line_name, AVG(se.passengers_on) AS avg_passengers
            FROM stop_events se
            JOIN trips t ON se.trip_id = t.trip_id
            JOIN lines l ON t.line_id = l.line_id
            GROUP BY l.line_name
            ORDER BY avg_passengers DESC
        """
    },
    "Q7": {
        "description": "Top 10 busiest stops",
        "sql": """
            SELECT s.stop_name, SUM(se.passengers_on + se.passengers_off) AS total_activity
            FROM stop_events se
            JOIN stops s ON se.stop_id = s.stop_id
            GROUP BY s.stop_name
            ORDER BY total_activity DESC
            LIMIT 10
        """
    },
    "Q8": {
        "description": "Count delays by line (>2 min late)",
        "sql": """
            SELECT l.line_name, COUNT(*) AS delay_count
            FROM stop_events se
            JOIN trips t ON se.trip_id = t.trip_id
            JOIN lines l ON t.line_id = l.line_id
            WHERE (se.actual_time - se.scheduled_time) > interval '2 minutes'
            GROUP BY l.line_name
            ORDER BY delay_count DESC
        """
    },
    "Q9": {
        "description": "Trips with 3+ delayed stops",
        "sql": """
            SELECT se.trip_id, COUNT(*) AS delayed_stop_count
            FROM stop_events se
            WHERE (se.actual_time - se.scheduled_time) > interval '2 minutes'
            GROUP BY se.trip_id
            HAVING COUNT(*) >= 3
            ORDER BY delayed_stop_count DESC
        """
    },
    "Q10": {
        "description": "Stops with above-average ridership",
        "sql": """
            SELECT s.stop_name, SUM(se.passengers_on) AS total_boardings
            FROM stop_events se
            JOIN stops s ON se.stop_id = s.stop_id
            GROUP BY s.stop_name
            HAVING SUM(se.passengers_on) > (
                SELECT AVG(total) FROM (
                    SELECT SUM(passengers_on) AS total
                    FROM stop_events
                    GROUP BY stop_id
                ) AS t
            )
            ORDER BY total_boardings DESC
        """
    }
}

def run_query(conn, query_id):
    if query_id not in QUERIES:
        print(f"Query {query_id} not found!")
        sys.exit(1)

    sql = QUERIES[query_id]["sql"]
    description = QUERIES[query_id]["description"]

    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

    results = [dict(zip(colnames, row)) for row in rows]
    return {
        "query": query_id,
        "description": description,
        "results": results,
        "count": len(results)
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Query ID (Q1..Q10)")
    parser.add_argument("--all", action="store_true", help="Run all queries")
    parser.add_argument("--dbname", required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="")
    parser.add_argument("--format", choices=["json", "table"], default="table")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.host,
        dbname=args.dbname,
        user=args.user,
        password=args.password
    )

    if args.all:
        output = []
        for qid in sorted(QUERIES.keys()):
            output.append(run_query(conn, qid))
    elif args.query:
        output = run_query(conn, args.query)
    else:
        parser.print_help()
        sys.exit(0)

    conn.close()

    if args.format == "json":
        print(json.dumps(output, indent=4, default=str))
    else:
        # Simple table output
        if isinstance(output, list):
            for q in output:
                print(f"--- {q['query']}: {q['description']} ---")
                for row in q["results"]:
                    print(row)
                print(f"Count: {q['count']}\n")
        else:
            print(f"--- {output['query']}: {output['description']} ---")
            for row in output["results"]:
                print(row)
            print(f"Count: {output['count']}")

if __name__ == "__main__":
    main()
