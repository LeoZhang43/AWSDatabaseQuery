import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from boto3.dynamodb.conditions import Key, Attr
import boto3

# ----------------------------
# DynamoDB setup
# ----------------------------
def get_table(table_name):
    return boto3.resource("dynamodb", region_name="us-east-2").Table(table_name)

# ----------------------------
# Query functions
# ----------------------------

def query_recent_papers(table_name, category, limit=10):
    """Get most recent papers in a category"""
    table = get_table(table_name)
    response = table.query(
        KeyConditionExpression=Key("PK").eq(f"CATEGORY#{category}"),
        ScanIndexForward=False,
        Limit=int(limit)
    )
    return response.get("Items", [])


def query_papers_by_author(table_name, author_name, limit=10):
    """Query papers by author using GSI AuthorIndex"""
    table = get_table(table_name)
    try:
        response = table.query(
            IndexName="AuthorIndex",
            KeyConditionExpression=Key("GSI1PK").eq(f"AUTHOR#{author_name}"),
            Limit=int(limit)
        )
        return response.get("Items", [])
    except Exception:
        # fallback if GSI missing
        response = table.scan(
            FilterExpression=Attr("authors").contains(author_name)
        )
        return response.get("Items", [])

def query_paper_by_id(table_name, paper_id):
    dynamodb = boto3.resource("dynamodb", region_name="us-east-2")
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={
        "PK": f"ARXIV#{paper_id}",
        "SK": f"ARXIV#{paper_id}"
    })
    return response.get("Item")

def search_papers_by_keyword(table_name, keyword, limit=10):
    """Search papers by keyword (via KeywordIndex if exists, else scan)"""
    table = get_table(table_name)
    try:
        response = table.query(
            IndexName="KeywordIndex",
            KeyConditionExpression=Key("GSI3PK").eq(f"KEYWORD#{keyword.lower()}"),
            Limit=int(limit)
        )
        return response.get("Items", [])
    except Exception:
        response = table.scan(
            FilterExpression=Attr("title").contains(keyword),
            Limit=int(limit)
        )
        return response.get("Items", [])


# ----------------------------
# HTTP Request Handler
# ----------------------------

class RequestHandler(BaseHTTPRequestHandler):
    table_name = None

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        try:
            # Recent papers in category
            if path == "/papers/recent":
                category = query.get("category", [None])[0]
                limit = query.get("limit", [10])[0]
                if not category:
                    self.send_error(400, "Missing 'category' parameter")
                    return
                items = query_recent_papers(self.table_name, category, limit)
                self.send_json(items)

            # Papers by author
            elif path == "/papers/author":
                author = query.get("name", [None])[0]
                limit = query.get("limit", [10])[0]
                if not author:
                    self.send_error(400, "Missing 'name' parameter")
                    return
                items = query_papers_by_author(self.table_name, author, limit)
                self.send_json(items)

            # Single paper by ID
            elif path == "/papers/id":
                paper_id = query.get("id", [None])[0]
                if not paper_id:
                    self.send_error(400, "Missing 'id' parameter")
                    return
                item = query_paper_by_id(self.table_name, paper_id)
                if not item:
                    self.send_error(404, "Paper not found")
                    return
                self.send_json(item)

            # Search by keyword
            elif path == "/papers/search":
                keyword = query.get("keyword", [None])[0]
                limit = query.get("limit", [10])[0]
                if not keyword:
                    self.send_error(400, "Missing 'keyword' parameter")
                    return
                items = search_papers_by_keyword(self.table_name, keyword, limit)
                self.send_json(items)

            else:
                self.send_error(404, "Endpoint not found")

        except Exception as e:
            print(f"❌ Error: {e}")
            self.send_error(500, f"Server error: {e}")

    # Utility: Send JSON response
    def send_json(self, data):
        body = json.dumps(data, indent=2, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

# ----------------------------
# Main entry
# ----------------------------
def run_server(port, table_name):
    RequestHandler.table_name = table_name
    server = HTTPServer(("0.0.0.0", port), RequestHandler)
    print(f"🚀 API server running on http://0.0.0.0:{port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down server...")
        server.server_close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--table", type=str, required=True)
    args = parser.parse_args()
    run_server(args.port, args.table)
