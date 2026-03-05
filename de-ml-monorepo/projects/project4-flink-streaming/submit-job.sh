#!/bin/bash
set -e

echo "Waiting for Flink JobManager to be ready..."
until $(curl --output /dev/null --silent --head --fail http://flink-jobmanager:8081); do
    printf '.'
    sleep 5
done

echo "Flink JobManager is ready!"

# Wait a bit more to ensure full startup
sleep 10

echo "Submitting Flink Streaming Job..."

# Submit the job to Flink cluster
/opt/flink/bin/flink run \
  --jobmanager flink-jobmanager:8081 \
  --class com.namazbek.dataeng.FlinkStreamingJob \
  /opt/flink/examples/flink-streaming-job-1.0.jar

echo "Job submitted successfully!"

# Keep container running to see logs
tail -f /opt/flink/log/*.log
