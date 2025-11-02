#!/bin/bash
# test.sh

./build.sh
./run.sh

echo ""
echo "Testing all queries..."
for i in {1..10}; do
    docker-compose run --rm app python queries.py --query Q$i --dbname transit --format json --password transit123 --user transit --host db
done

docker-compose down