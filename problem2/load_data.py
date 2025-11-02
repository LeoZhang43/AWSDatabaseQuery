import json
import argparse
import boto3
import re
from collections import Counter
from itertools import chain

STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
    'can', 'this', 'that', 'these', 'those', 'we', 'our', 'use', 'using',
    'based', 'approach', 'method', 'paper', 'propose', 'proposed', 'show'
}

def extract_keywords(text, top_n=10):
    words = re.findall(r'\b\w+\b', text.lower())
    words = [w for w in words if w not in STOPWORDS and len(w) > 2]
    counter = Counter(words)
    return [w for w, _ in counter.most_common(top_n)]

def create_table(dynamodb, table_name):
    # Check if table exists
    try:
        table = dynamodb.Table(table_name)
        table.load()  # Raises exception if table does not exist
        print(f"Table {table_name} already exists. Deleting it...")
        table.delete()
        table.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)
        print(f"Deleted existing table: {table_name}")
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        pass  # Table does not exist, continue

    print(f"Creating DynamoDB table: {table_name}")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'PK', 'KeyType': 'HASH'},
            {'AttributeName': 'SK', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'PK', 'AttributeType': 'S'},
            {'AttributeName': 'SK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI1PK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI1SK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI2PK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI2SK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI3PK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI3SK', 'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'AuthorIndex',
                'KeySchema': [
                    {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                    {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            {
                'IndexName': 'PaperIdIndex',
                'KeySchema': [
                    {'AttributeName': 'GSI2PK', 'KeyType': 'HASH'},
                    {'AttributeName': 'GSI2SK', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            {
                'IndexName': 'KeywordIndex',
                'KeySchema': [
                    {'AttributeName': 'GSI3PK', 'KeyType': 'HASH'},
                    {'AttributeName': 'GSI3SK', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    print("Table created successfully.")
    return table


def transform_paper(paper):
    """
    Convert a single paper JSON into multiple DynamoDB items
    for main table + GSIs (author, keyword, arxiv_id).
    """
    keywords = extract_keywords(paper.get('abstract', ''))
    items = []

    arxiv_id = paper['arxiv_id']
    published_date = paper['published'][:10]  # YYYY-MM-DD

    # --- Category items (Main table) ---
    for category in paper.get('categories', []):
        items.append({
            'PK': f"CATEGORY#{category}",
            'SK': f"{published_date}#{arxiv_id}",
            'arxiv_id': arxiv_id,
            'title': paper['title'],
            'authors': paper['authors'],
            'abstract': paper['abstract'],
            'categories': paper['categories'],
            'keywords': keywords,
            'published': paper['published'],
            'GSI2PK': f"ARXIV#{arxiv_id}",
            'GSI2SK': f"CATEGORY#{category}"
        })

    # --- Author items (AuthorIndex GSI) ---
    for author in paper.get('authors', []):
        items.append({
            'PK': f"ARXIV#{arxiv_id}",   # dummy PK
            'SK': f"AUTHOR#{author}#{published_date}",
            'GSI1PK': f"AUTHOR#{author}",
            'GSI1SK': f"{published_date}#{arxiv_id}",
            'arxiv_id': arxiv_id,
            'title': paper['title'],
            'authors': paper['authors'],  # <-- add this line
            'categories': paper['categories'],
            'published': paper['published']
        })

    # --- Keyword items (KeywordIndex GSI) ---
    for kw in keywords:
        items.append({
            'PK': f"ARXIV#{arxiv_id}",   # dummy PK
            'SK': f"KEYWORD#{kw}#{published_date}",
            'GSI3PK': f"KEYWORD#{kw}",
            'GSI3SK': f"{published_date}#{arxiv_id}",
            'arxiv_id': arxiv_id,
            'title': paper['title'],
            'authors': paper['authors'],
            'categories': paper['categories'],
            'published': paper['published']
        })

    # --- Paper ID item (PaperIdIndex GSI) ---
    items.append({
        'PK': f"ARXIV#{arxiv_id}",
        'SK': f"ARXIV#{arxiv_id}",
        'GSI2PK': f"ARXIV#{arxiv_id}",
        'GSI2SK': f"ARXIV#{arxiv_id}",
        'arxiv_id': arxiv_id,
        'title': paper['title'],
        'authors': paper['authors'],
        'abstract': paper['abstract'],
        'categories': paper['categories'],
        'keywords': keywords,
        'published': paper['published']
    })

    return items

def batch_write(table, items):
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('papers_json_path')
    parser.add_argument('table_name')
    parser.add_argument('--region', default='us-east-1')
    args = parser.parse_args()

    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    table = create_table(dynamodb, args.table_name)

    print("Loading papers from JSON...")
    with open(args.papers_json_path, 'r', encoding='utf-8') as f:
        papers = json.load(f)

    all_items = list(chain.from_iterable(transform_paper(p) for p in papers))
    batch_write(table, all_items)

    # Reporting
    num_papers = len(papers)
    total_items = len(all_items)
    denorm_factor = total_items / num_papers

    # Breakdown
    category_items = sum(len(p.get('categories', [])) for p in papers)
    author_items = sum(len(p.get('authors', [])) for p in papers)
    keyword_items = sum(len(extract_keywords(p.get('abstract', ''))) for p in papers)
    paper_id_items = num_papers

    print(f"\nLoaded {num_papers} papers")
    print(f"Created {total_items} DynamoDB items (denormalized)")
    print(f"Denormalization factor: {denorm_factor:.1f}x\n")
    print("Storage breakdown:")
    print(f"  - Category items: {category_items} ({category_items/num_papers:.1f} per paper avg)")
    print(f"  - Author items: {author_items} ({author_items/num_papers:.1f} per paper avg)")
    print(f"  - Keyword items: {keyword_items} ({keyword_items/num_papers:.1f} per paper avg)")
    print(f"  - Paper ID items: {paper_id_items} (1.0 per paper)")

if __name__ == '__main__':
    main()
