#!/bin/bash
set -e

echo "Initializing HDFS directories and permissions..."

# Wait a bit for HDFS to be fully ready
sleep 10

# Create necessary directories
echo "Creating HDFS directories..."

hdfs dfs -mkdir -p /data/raw/transactions
hdfs dfs -mkdir -p /data/logs  
hdfs dfs -mkdir -p /data/reference/users
hdfs dfs -mkdir -p /data/processed
hdfs dfs -mkdir -p /data/analytics
hdfs dfs -mkdir -p /tmp/hadoop-batch-jobs

# Set permissions
echo "Setting HDFS permissions..."
hdfs dfs -chmod 755 /data
hdfs dfs -chmod 755 /data/raw
hdfs dfs -chmod 755 /data/logs
hdfs dfs -chmod 755 /data/reference
hdfs dfs -chmod 755 /data/processed
hdfs dfs -chmod 755 /data/analytics
hdfs dfs -chmod 777 /tmp/hadoop-batch-jobs

echo "HDFS initialization completed!"

# Show directory structure
echo "HDFS directory structure:"
hdfs dfs -ls -R /data
