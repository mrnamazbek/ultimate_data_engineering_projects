FROM apache/hadoop:3.3.6

# Install Python and required packages
USER root
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt

# Set up Hadoop environment
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Copy Hadoop configuration files
COPY conf/ $HADOOP_CONF_DIR/

# Copy Java MapReduce jobs
COPY target/hadoop-batch-jobs-1.0.jar /opt/hadoop-jobs/
COPY target/lib/ /opt/hadoop-jobs/lib/

# Copy Python scripts
COPY scripts/ /opt/scripts/
RUN chmod +x /opt/scripts/*.sh

# Copy sample data
COPY data/ /opt/sample-data/

# Create HDFS directories and copy sample data
COPY init-hdfs.sh /opt/init-hdfs.sh
RUN chmod +x /opt/init-hdfs.sh

# Set working directory
WORKDIR /opt/scripts

# Run the batch processing pipeline
CMD ["/opt/scripts/run-pipeline.sh"]
