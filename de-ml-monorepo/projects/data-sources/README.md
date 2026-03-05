# Big Data Sources Generator

## Goal
Generate realistic, large-scale datasets for testing and demonstrating the capabilities of our big data platform across all projects.

## Features
- **Multi-format data generation**: JSON, CSV, Parquet, Avro
- **Various data types**: Transactional, streaming, logs, IoT sensors, social media
- **Configurable volume**: From MBs to TBs of test data
- **Realistic patterns**: Seasonal trends, user behavior, error patterns
- **Multiple destinations**: Kafka topics, HDFS, MinIO, databases

## Data Types Generated

### 1. E-commerce Transactions
- User purchases, refunds, subscriptions
- Geographic distribution
- Seasonal patterns and trends
- Payment processing data

### 2. Application Logs
- Multi-level logging (INFO, WARN, ERROR, DEBUG)
- Different service components
- Error patterns and anomalies
- Performance metrics

### 3. IoT Sensor Data
- Temperature, humidity, pressure sensors
- GPS location tracking
- Device status and health metrics
- Time-series data with noise

### 4. Social Media Events
- User interactions (likes, shares, comments)
- Content creation and consumption
- Trending topics and hashtags
- User engagement patterns

### 5. Financial Market Data
- Stock prices, trading volumes
- Market indicators and indices
- Currency exchange rates
- Real-time price movements

## Usage

```bash
# Generate 1GB of mixed data types
docker-compose --profile bigdata run --rm data-generator

# Environment variables for customization:
# BIG_DATA_RATE=1000     # Events per second
# DATA_VOLUME_GB=10      # Total volume to generate
```

## Configuration
All generation parameters can be configured via environment variables:
- Volume, rate, data types, formats, destinations
- Realistic error injection and anomaly patterns
- Geographic and temporal distribution patterns
