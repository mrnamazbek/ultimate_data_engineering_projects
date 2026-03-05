FROM apache/flink:1.18-java11

# Install dependencies for Kafka and PostgreSQL connectors
RUN mkdir -p /opt/flink/lib-ext && \
    cd /opt/flink/lib-ext && \
    curl -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.18.0/flink-connector-kafka-1.18.0.jar && \
    curl -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1/flink-connector-jdbc-3.1.1.jar && \
    curl -O https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar && \
    curl -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.18.0/flink-json-1.18.0.jar

# Copy our streaming job
COPY target/flink-streaming-job-1.0.jar /opt/flink/examples/

# Copy the job submission script
COPY submit-job.sh /opt/submit-job.sh
RUN chmod +x /opt/submit-job.sh

WORKDIR /opt/flink

# Submit the job when container starts
CMD ["/opt/submit-job.sh"]
