# Paper Discovery System - Problem 2

## Schema Design Decisions

### Partition Key Structure
- **Main Table Partition Key (`PK`)**: `CATEGORY#<category>`  
- **Sort Key (`SK`)**: `<published_date>#<arxiv_id>`  
- **Reasoning**:  
  - Enables efficient queries for **recent papers by category**.  
  - Sort key allows **range queries by date** within a category.

### Global Secondary Indexes (GSIs)
- **GSI1 (AuthorIndex)**:  
  - `GSI1PK`: `AUTHOR#<author_name>`  
  - `GSI1SK`: `<published_date>#<arxiv_id>`  
  - Supports **querying all papers by a specific author**.
- **GSI2 (PaperIdIndex)**:  
  - `GSI2PK`: `PAPER#<arxiv_id>`  
  - `GSI2SK`: `<arxiv_id>`  
  - Supports **direct lookup of a paper by ID**.
- **GSI3 (KeywordIndex)**:  
  - `GSI3PK`: `KEYWORD#<keyword>`  
  - `GSI3SK`: `<published_date>#<arxiv_id>`  
  - Supports **searching papers by keyword**.

### Denormalization Trade-offs
- **Multiple items per paper** for:
  - Each **category**
  - Each **author**
  - Each **keyword**  
- **Trade-off**: increases storage size and write cost but allows **fast, simple read queries**.  
- Avoids **complex joins**, which DynamoDB does not support efficiently.

---

## Denormalization Analysis

- **Average items per paper**: ~14.9  
- **Storage multiplication factor**: ~15×  
- **Most duplication**:
  - **Keywords** (10 items per paper)  
  - **Authors** (5 items per paper)  

---

## Query Limitations

### Inefficient Queries
- **Count total papers by author**  
- **Find most cited papers globally**  
- **Aggregate queries across categories or keywords**  

### Why Difficult in DynamoDB
- DynamoDB **does not support joins or ad-hoc aggregation** efficiently.  
- Queries are limited to **single partition key / sort key or indexes**.  
- Global aggregation would require **scanning the full table**, which is slow and costly.

---

## When to Use DynamoDB

- **Use cases**:
  - High throughput for **key-value or single-table access patterns**  
  - Real-time applications with **predictable queries**  
  - Scalable, serverless, low-latency read/write  

- **Trade-offs**:
  - **Pros**: fast, horizontally scalable, serverless, pay-per-request  
  - **Cons**: limited querying flexibility, denormalization required, eventual consistency in some cases  

---

## EC2 Deployment

- **Public IP**: `3.134.99.45`  
- **IAM Role ARN**: `arn:aws:iam::<account-id>:role/<role-name>`  
- **Challenges encountered**:
  - SSH key permission issues on WSL  
  - Opening custom port 8080 in security group for API access  
  - Boto3 unable to locate AWS credentials before attaching IAM role  
  - Adjusting API server `argparse` arguments for correct port and table  

---

## References
- [AWS DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)  
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)  
