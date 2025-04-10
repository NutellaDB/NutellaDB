#!/usr/bin/env bash
set -euo pipefail

# Start server
echo "⏳ Starting server..."
go run . startserver &
server_pid=$!

# Wait for server to be ready
sleep 2

base="http://localhost:3000"

db="test_$$"                 
coll="fruits"

echo "⏳ creating DB $db"
curl -sf -X POST $base/create-db        -H "Content-Type: application/json" \
     -d "{\"dbID\":\"$db\"}"            | jq .

echo "⏳ creating collection with order 4"
curl -sf -X POST $base/create-collection -H "Content-Type: application/json" \
     -d "{\"dbID\":\"$db\",\"name\":\"$coll\",\"order\":4}" | jq .

# Insert multiple keys to create a multi-node btree
echo "⏳ inserting multiple keys to create a multi-node btree"
fruits=("apple:red" "banana:yellow" "cherry:red" "date:brown" "elderberry:purple" \
        "fig:purple" "grape:purple" "honeydew:green" "kiwi:brown" "lemon:yellow")

for pair in "${fruits[@]}"; do
    key="${pair%%:*}"
    value="${pair##*:}"
    echo "Inserting $key: $value"
    curl -sf -X POST $base/insert -H "Content-Type: application/json" \
         -d "{\"dbID\":\"$db\",\"collection\":\"$coll\",\"key\":\"$key\",\"value\":\"$value\"}" | jq .
done

# Test reading multiple keys
echo "⏳ reading multiple keys"
for pair in "${fruits[@]}"; do
    key="${pair%%:*}"
    echo "Reading $key"
    curl -sf "$base/find?dbID=$db&collection=$coll&key=$key" | jq .
done

# Test updating multiple keys
echo "⏳ updating multiple keys"
for pair in "${fruits[@]}"; do
    key="${pair%%:*}"
    echo "Updating $key"
    curl -sf -X POST $base/update -H "Content-Type: application/json" \
         -d "{\"dbID\":\"$db\",\"collection\":\"$coll\",\"key\":\"$key\",\"value\":\"modified\"}" | jq .
done

# Test deleting multiple keys
echo "⏳ deleting multiple keys"
for pair in "${fruits[@]}"; do
    key="${pair%%:*}"
    echo "Deleting $key"
    curl -sf -X DELETE "$base/delete?dbID=$db&collection=$coll&key=$key" | jq .
done

echo "✅ multi-node btree test finished"

# Cleanup - kill the server
kill $server_pid
