import argparse
import boto3
import json
import time
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')

def query_recent_in_category(table_name, category, limit=20):
    table = dynamodb.Table(table_name)
    start_time = time.time()
    response = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,  # descending
        Limit=limit
    )
    exec_time = int((time.time() - start_time) * 1000)
    items = [{
        'arxiv_id': i['arxiv_id'],
        'title': i['title'],
        'authors': i['authors'],
        'published': i['published'],
        'categories': i['categories']
    } for i in response['Items']]
    return {
        "query_type": "recent_in_category",
        "parameters": {"category": category, "limit": limit},
        "results": items,
        "count": len(items),
        "execution_time_ms": exec_time
    }

def query_papers_by_author(table_name, author_name):
    table = dynamodb.Table(table_name)
    start_time = time.time()
    response = table.query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    exec_time = int((time.time() - start_time) * 1000)
    items = [{
        'arxiv_id': i['arxiv_id'],
        'title': i['title'],
        'authors': i['authors'],
        'published': i['published'],
        'categories': i['categories']
    } for i in response['Items']]
    return {
        "query_type": "papers_by_author",
        "parameters": {"author_name": author_name},
        "results": items,
        "count": len(items),
        "execution_time_ms": exec_time
    }

def get_paper_by_id(table_name, arxiv_id):
    table = dynamodb.Table(table_name)
    start_time = time.time()
    response = table.query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'ARXIV#{arxiv_id}')
    )
    exec_time = int((time.time() - start_time) * 1000)
    if not response['Items']:
        result = None
    else:
        i = response['Items'][0]
        result = {
            'arxiv_id': i['arxiv_id'],
            'title': i['title'],
            'authors': i['authors'],
            'published': i['published'],
            'categories': i.get('categories', [])
        }
    return {
        "query_type": "get_paper_by_id",
        "parameters": {"arxiv_id": arxiv_id},
        "results": [result] if result else [],
        "count": 1 if result else 0,
        "execution_time_ms": exec_time
    }

def query_papers_in_date_range(table_name, category, start_date, end_date):
    table = dynamodb.Table(table_name)
    start_time = time.time()
    response = table.query(
        KeyConditionExpression=(
            Key('PK').eq(f'CATEGORY#{category}') &
            Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzzz')
        )
    )
    exec_time = int((time.time() - start_time) * 1000)
    items = [{
        'arxiv_id': i['arxiv_id'],
        'title': i['title'],
        'authors': i['authors'],
        'published': i['published'],
        'categories': i['categories']
    } for i in response['Items']]
    return {
        "query_type": "papers_in_date_range",
        "parameters": {"category": category, "start_date": start_date, "end_date": end_date},
        "results": items,
        "count": len(items),
        "execution_time_ms": exec_time
    }

def query_papers_by_keyword(table_name, keyword, limit=20):
    table = dynamodb.Table(table_name)
    start_time = time.time()
    response = table.query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=limit
    )
    exec_time = int((time.time() - start_time) * 1000)
    items = [{
        'arxiv_id': i['arxiv_id'],
        'title': i['title'],
        'authors': i['authors'],
        'published': i['published'],
        'categories': i['categories']
    } for i in response['Items']]
    return {
        "query_type": "papers_by_keyword",
        "parameters": {"keyword": keyword, "limit": limit},
        "results": items,
        "count": len(items),
        "execution_time_ms": exec_time
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['recent', 'author', 'get', 'daterange', 'keyword'])
    parser.add_argument('args', nargs='*')
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--table', default='arxiv-papers')
    args = parser.parse_args()

    if args.command == 'recent':
        category = args.args[0]
        output = query_recent_in_category(args.table, category, limit=args.limit)
    elif args.command == 'author':
        author_name = args.args[0]
        output = query_papers_by_author(args.table, author_name)
    elif args.command == 'get':
        arxiv_id = args.args[0]
        output = get_paper_by_id(args.table, arxiv_id)
    elif args.command == 'daterange':
        category, start_date, end_date = args.args[:3]
        output = query_papers_in_date_range(args.table, category, start_date, end_date)
    elif args.command == 'keyword':
        keyword = args.args[0]
        output = query_papers_by_keyword(args.table, keyword, limit=args.limit)
    else:
        raise ValueError("Unknown command")

    print(json.dumps(output, indent=2))

if __name__ == '__main__':
    main()
