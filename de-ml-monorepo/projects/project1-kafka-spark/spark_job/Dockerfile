FROM bitnami/spark:3.5

USER root
# Install PostgreSQL JDBC driver
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar

# Install Hadoop AWS / S3A connector (same version as bundled Hadoop)
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar

COPY streaming_job.py /opt/spark-job/streaming_job.py

CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "/opt/spark-job/streaming_job.py"]
