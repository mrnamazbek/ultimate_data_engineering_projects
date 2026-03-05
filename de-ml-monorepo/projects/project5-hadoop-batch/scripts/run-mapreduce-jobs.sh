#!/bin/bash
set -e

echo "Running Hadoop MapReduce jobs..."

JAR_PATH="/opt/hadoop-jobs/hadoop-batch-jobs-1.0.jar"
HDFS_INPUT="/data"
HDFS_OUTPUT="/data/processed"

# Check if JAR exists
if [ ! -f "$JAR_PATH" ]; then
    echo "Error: JAR file not found at $JAR_PATH"
    echo "Please build the project first: mvn clean package"
    exit 1
fi

# Clean up previous output directories
echo "Cleaning up previous output directories..."
hdfs dfs -rm -r -f $HDFS_OUTPUT/log-analysis 2>/dev/null || true
hdfs dfs -rm -r -f $HDFS_OUTPUT/transaction-aggregation 2>/dev/null || true

# 1. Run Log Analysis Job
echo "Running Log Analysis MapReduce job..."
hadoop jar $JAR_PATH \
    com.namazbek.dataeng.mapreduce.LogAnalysisJob \
    $HDFS_INPUT/logs \
    $HDFS_OUTPUT/log-analysis

if [ $? -eq 0 ]; then
    echo "✓ Log Analysis job completed successfully"
    echo "Results preview:"
    hdfs dfs -cat $HDFS_OUTPUT/log-analysis/part-* | head -10
else
    echo "✗ Log Analysis job failed"
    exit 1
fi

echo ""

# 2. Run Transaction Aggregation Job  
echo "Running Transaction Aggregation MapReduce job..."
hadoop jar $JAR_PATH \
    com.namazbek.dataeng.mapreduce.TransactionAggregationJob \
    $HDFS_INPUT/raw/transactions/*/data.json \
    $HDFS_OUTPUT/transaction-aggregation

if [ $? -eq 0 ]; then
    echo "✓ Transaction Aggregation job completed successfully"
    echo "Results preview:"
    hdfs dfs -cat $HDFS_OUTPUT/transaction-aggregation/part-* | head -10
else
    echo "✗ Transaction Aggregation job failed"
    exit 1
fi

echo ""

# 3. Show job statistics
echo "MapReduce Job Statistics:"
echo "========================"

LOG_OUTPUT_SIZE=$(hdfs dfs -du -s $HDFS_OUTPUT/log-analysis 2>/dev/null | awk '{print $1}')
TXN_OUTPUT_SIZE=$(hdfs dfs -du -s $HDFS_OUTPUT/transaction-aggregation 2>/dev/null | awk '{print $1}')

echo "Log Analysis output size: $(($LOG_OUTPUT_SIZE / 1024 / 1024)) MB"
echo "Transaction Aggregation output size: $(($TXN_OUTPUT_SIZE / 1024 / 1024)) MB"

# Count output records
LOG_RECORDS=$(hdfs dfs -cat $HDFS_OUTPUT/log-analysis/part-* 2>/dev/null | wc -l)
TXN_RECORDS=$(hdfs dfs -cat $HDFS_OUTPUT/transaction-aggregation/part-* 2>/dev/null | wc -l)

echo "Log Analysis records: $LOG_RECORDS"
echo "Transaction Aggregation records: $TXN_RECORDS"

echo ""
echo "All MapReduce jobs completed successfully!"
echo "Results are available in HDFS at: $HDFS_OUTPUT"
