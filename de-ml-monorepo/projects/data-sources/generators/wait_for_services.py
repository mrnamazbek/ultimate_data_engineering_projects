#!/usr/bin/env python3
"""
Wait for services to be ready before starting data generation.
"""

import os
import time
import socket
import logging
from kafka import KafkaProducer
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def wait_for_tcp_service(host, port, timeout=300):
    """Wait for TCP service to be available."""
    logger.info(f"Waiting for {host}:{port}...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                logger.info(f"✓ {host}:{port} is ready")
                return True
        except Exception:
            pass
        
        time.sleep(5)
    
    logger.error(f"✗ {host}:{port} not ready after {timeout} seconds")
    return False

def wait_for_http_service(url, timeout=300):
    """Wait for HTTP service to be available."""
    logger.info(f"Waiting for {url}...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code < 500:
                logger.info(f"✓ {url} is ready")
                return True
        except Exception:
            pass
        
        time.sleep(5)
    
    logger.error(f"✗ {url} not ready after {timeout} seconds")
    return False

def wait_for_kafka():
    """Wait for Kafka to be ready."""
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
    logger.info(f"Checking Kafka connectivity: {kafka_broker}")
    
    for attempt in range(30):  # 5 minutes
        try:
            producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                request_timeout_ms=5000,
                retries=0
            )
            # Get cluster metadata
            metadata = producer.list_consumer_groups()
            producer.close()
            logger.info("✓ Kafka is ready")
            return True
        except Exception as e:
            if attempt == 0:
                logger.info(f"Waiting for Kafka... ({e})")
            time.sleep(10)
    
    logger.error("✗ Kafka not ready")
    return False

def main():
    """Wait for all required services."""
    logger.info("Checking service availability...")
    
    services_ready = True
    
    # Parse service endpoints
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
    hdfs_namenode = os.getenv('HDFS_NAMENODE', 'namenode:8020')
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    
    kafka_host, kafka_port = kafka_broker.split(':')
    hdfs_host, hdfs_port = hdfs_namenode.split(':')
    minio_host, minio_port = minio_endpoint.split(':')
    
    # Check Kafka
    if not wait_for_kafka():
        services_ready = False
    
    # Check HDFS NameNode
    if not wait_for_http_service(f"http://{hdfs_host}:9870"):
        services_ready = False
    
    # Check MinIO
    if not wait_for_http_service(f"http://{minio_host}:{minio_port}/minio/health/live"):
        services_ready = False
    
    if services_ready:
        logger.info("✓ All services are ready!")
        return True
    else:
        logger.error("✗ Some services are not ready")
        return False

if __name__ == "__main__":
    if not main():
        exit(1)
