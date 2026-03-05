FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# Copy generator scripts
COPY generators/ /opt/generators/
COPY config/ /opt/config/
COPY run-generators.sh /opt/run-generators.sh

RUN chmod +x /opt/run-generators.sh

WORKDIR /opt

# Run the data generation pipeline
CMD ["/opt/run-generators.sh"]
