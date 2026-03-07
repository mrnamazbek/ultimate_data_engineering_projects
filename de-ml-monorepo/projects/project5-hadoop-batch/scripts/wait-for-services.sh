#!/bin/bash
set -e

echo "Waiting for Hadoop services to be ready..."

# Wait for NameNode
echo "Waiting for HDFS NameNode..."
until curl -f http://namenode:9870/ >/dev/null 2>&1; do
    echo "NameNode not ready yet..."
    sleep 5
done
echo "✓ HDFS NameNode is ready"

# Wait for YARN ResourceManager  
echo "Waiting for YARN ResourceManager..."
until curl -f http://yarn-resourcemanager:8088/ >/dev/null 2>&1; do
    echo "YARN ResourceManager not ready yet..."
    sleep 5
done
echo "✓ YARN ResourceManager is ready"

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
until pg_isready -h postgres -p 5432 -U gemini >/dev/null 2>&1; do
    echo "PostgreSQL not ready yet..."
    sleep 5
done
echo "✓ PostgreSQL is ready"

echo "All Hadoop services are ready!"
