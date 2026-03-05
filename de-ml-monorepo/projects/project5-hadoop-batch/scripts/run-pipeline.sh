#!/bin/bash
set -e

echo "Starting Hadoop Batch Processing Pipeline..."

# Wait for HDFS and YARN to be ready
echo "Waiting for Hadoop services..."
/opt/scripts/wait-for-services.sh

# Initialize HDFS directories and sample data
echo "Initializing HDFS..."
/opt/init-hdfs.sh

# Run data ingestion
echo "Running data ingestion..."
python3 /opt/scripts/data_ingestion.py

# Run MapReduce jobs
echo "Running MapReduce jobs..."
/opt/scripts/run-mapreduce-jobs.sh

# Run Hive analytics
echo "Running Hive analytics..."
python3 /opt/scripts/hive_analytics.py

# Export results to PostgreSQL
echo "Exporting results to PostgreSQL..."
python3 /opt/scripts/export_results.py

echo "Hadoop batch processing pipeline completed successfully!"
