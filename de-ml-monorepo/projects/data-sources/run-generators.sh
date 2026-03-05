#!/bin/bash
set -e

echo "Starting Big Data Sources Generator..."

# Configuration
DATA_RATE=${BIG_DATA_RATE:-100}
TOTAL_VOLUME_GB=${DATA_VOLUME_GB:-10}
KAFKA_BROKER=${KAFKA_BROKER:-kafka:9092}
HDFS_NAMENODE=${HDFS_NAMENODE:-namenode:8020}
MINIO_ENDPOINT=${MINIO_ENDPOINT:-minio:9000}

echo "Configuration:"
echo "  Data Rate: $DATA_RATE events/sec"
echo "  Total Volume: $TOTAL_VOLUME_GB GB"
echo "  Kafka Broker: $KAFKA_BROKER"
echo "  HDFS NameNode: $HDFS_NAMENODE"
echo "  MinIO Endpoint: $MINIO_ENDPOINT"

# Wait for services
echo "Waiting for services to be ready..."
python3 /opt/generators/wait_for_services.py

# Generate different types of data in parallel
echo "Starting data generation processes..."

# 1. Real-time streaming data to Kafka
echo "Starting streaming data generator..."
python3 /opt/generators/streaming_data_generator.py &
STREAMING_PID=$!

# 2. Batch data to HDFS
echo "Starting batch data generator..."  
python3 /opt/generators/batch_data_generator.py &
BATCH_PID=$!

# 3. Log files generator
echo "Starting log files generator..."
python3 /opt/generators/log_generator.py &
LOG_PID=$!

# 4. IoT sensor data
echo "Starting IoT data generator..."
python3 /opt/generators/iot_generator.py &
IOT_PID=$!

# 5. Financial market data
echo "Starting market data generator..."
python3 /opt/generators/market_data_generator.py &
MARKET_PID=$!

echo "All generators started. PIDs: $STREAMING_PID $BATCH_PID $LOG_PID $IOT_PID $MARKET_PID"

# Function to handle cleanup
cleanup() {
    echo "Stopping all generators..."
    kill $STREAMING_PID $BATCH_PID $LOG_PID $IOT_PID $MARKET_PID 2>/dev/null || true
    wait
    echo "Data generation completed!"
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Monitor progress and run for specified duration
python3 /opt/generators/monitor_progress.py &
MONITOR_PID=$!

# Calculate runtime based on data volume and rate
RUNTIME_SECONDS=$(python3 -c "print(int($TOTAL_VOLUME_GB * 1024 * 1024 * 1024 / ($DATA_RATE * 1000)))")
echo "Estimated runtime: $RUNTIME_SECONDS seconds"

# Wait for completion or timeout
sleep $RUNTIME_SECONDS

# Cleanup
kill $MONITOR_PID 2>/dev/null || true
cleanup
