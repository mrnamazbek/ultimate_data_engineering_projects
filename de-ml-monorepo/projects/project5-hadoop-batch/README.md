# Project 5 – Hadoop Batch Processing & Analytics

## Goal
Demonstrate enterprise-scale batch processing using Hadoop ecosystem for petabyte-scale data analytics, ETL workflows, and data lake operations.

## Stack
- **Apache Hadoop 3.3.6** – Distributed storage (HDFS) and computing (YARN)
- **Apache Hive 4.0** – SQL-like queries on big data
- **Java MapReduce** – Custom distributed computing jobs
- **Python** – Data ingestion and orchestration scripts
- **HDFS** – Fault-tolerant distributed file system

## Why Hadoop in Modern Stack?
Despite newer technologies, Hadoop remains critical for:
- **Petabyte-scale storage** with fault tolerance
- **Legacy enterprise systems** still run on Hadoop
- **Cost-effective storage** for long-term data retention
- **Batch processing** where latency is not critical
- **Data lake foundation** for various analytics workloads

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Hadoop Ecosystem                        │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │    HDFS     │    │    YARN     │    │    Hive     │     │
│  │ Distributed │    │  Resource   │    │   Query     │     │
│  │ File System │    │  Manager    │    │  Engine     │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│         │                   │                   │          │
│         └───────────────────┼───────────────────┘          │
│                             │                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           MapReduce Jobs                            │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │ ETL Process │  │Log Analysis │  │Aggregations │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
│                             │                              │
│                    ┌─────────────┐                        │
│                    │ PostgreSQL  │                        │
│                    │  Results    │                        │
│                    └─────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

## How to run

### Start Hadoop ecosystem
```bash
# From monorepo root:
docker-compose up --build namenode datanode yarn-resourcemanager yarn-nodemanager hive-metastore postgres
```

### Run batch processing job
```bash
docker-compose --profile hadoop run --rm hadoop-batch-job
```

### Access Hadoop UIs
- **HDFS NameNode**: http://localhost:9870
- **YARN ResourceManager**: http://localhost:8088
- **Hive Metastore**: Port 9083

## Features Implemented

### 1. Large-Scale Data Ingestion
- **Multi-format support**: CSV, JSON, Parquet, ORC
- **Partitioned storage**: Optimized for query performance
- **Compression**: Snappy, GZIP, LZ4 for storage efficiency
- **Schema evolution**: Handle changing data structures

### 2. MapReduce Jobs
- **Log analysis**: Parse and aggregate application logs
- **ETL pipelines**: Transform raw data to analytics-ready format
- **Deduplication**: Remove duplicate records at scale
- **Data quality**: Validation and cleansing workflows

### 3. Hive Analytics
- **External tables**: Query data without moving it
- **Partitioning**: Time-based and categorical partitions
- **Bucketing**: Optimize joins and aggregations
- **UDFs**: Custom functions for business logic

### 4. HDFS Operations
- **Replication factor**: 3x redundancy for fault tolerance
- **Block size optimization**: 128MB blocks for big data
- **Namespace management**: Organize data hierarchically
- **Quota management**: Control storage usage

## Sample Workflows

### Daily ETL Pipeline
```sql
-- Hive query for daily user activity aggregation
CREATE TABLE user_daily_activity
PARTITIONED BY (date_partition STRING)
STORED AS PARQUET
AS SELECT 
    user_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT session_id) as session_count,
    AVG(processing_time) as avg_processing_time,
    date_partition
FROM raw_events 
WHERE date_partition = '2024-03-05'
GROUP BY user_id, date_partition;
```

### Log Analysis MapReduce
```bash
# Custom MapReduce job for error pattern analysis
hadoop jar /opt/hadoop-jobs/log-analyzer.jar \
  -input /data/logs/2024/03/* \
  -output /results/error-analysis \
  -pattern "ERROR|FATAL"
```

## Performance Characteristics

### Throughput Metrics
- **HDFS**: 100+ MB/s per DataNode
- **MapReduce**: 10-100 GB/hour processing
- **Hive**: 1-10 TB/day analytics workloads
- **Scalability**: Linear scaling with node addition

### Storage Efficiency
- **Compression ratio**: 4-10x with Parquet + Snappy
- **Partition pruning**: 90%+ query speedup
- **Columnar storage**: 5-20x faster analytics queries

## Trade-offs & Limitations

### Advantages
- **Massive scale**: Proven at exabyte scale
- **Fault tolerance**: Automatic failure handling
- **Cost effective**: Commodity hardware
- **Ecosystem maturity**: Rich tooling and community
- **Data locality**: Computation moves to data

### Disadvantages
- **High latency**: Minutes to hours for results
- **Complex operations**: Heavy operational overhead
- **Resource intensive**: Requires dedicated clusters
- **Learning curve**: Complex distributed system concepts
- **Legacy technology**: Being superseded by cloud solutions

## Modern Alternative Comparison

| Aspect | Hadoop | Cloud (S3+EMR) | Spark on K8s |
|--------|--------|----------------|--------------|
| Setup | Complex | Managed | Moderate |
| Cost | CapEx | OpEx | OpEx |
| Scale | Petabytes | Unlimited | TBs-PBs |
| Latency | High | Medium | Low-Medium |
| Ops | Manual | Automated | Semi-automated |

## Monitoring & Troubleshooting
- YARN application tracking and resource usage
- HDFS health monitoring and block reports
- MapReduce job progress and performance metrics
- Hive query optimization and execution plans
- Custom business metrics exported to PostgreSQL
