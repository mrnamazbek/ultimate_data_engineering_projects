#!/usr/bin/env python3
"""
Hadoop Batch Data Ingestion Script
Demonstrates large-scale data ingestion patterns for big data processing.
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from hdfs3 import HDFileSystem
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
HDFS_HOST = os.getenv('HDFS_NAMENODE', 'namenode:8020')
HDFS_USER = 'hadoop'

class HadoopDataIngestion:
    def __init__(self):
        self.hdfs = HDFileSystem(host=HDFS_HOST.split(':')[0], port=int(HDFS_HOST.split(':')[1]))
        logger.info(f"Connected to HDFS: {HDFS_HOST}")
    
    def generate_large_dataset(self, num_records=1000000):
        """
        Generate a large synthetic dataset for batch processing demonstration.
        Simulates real-world data patterns with multiple data types.
        """
        logger.info(f"Generating {num_records:,} records...")
        
        # Generate base data
        np.random.seed(42)  # For reproducible results
        
        # User data
        user_ids = np.random.randint(1, 100000, num_records)
        
        # Transaction data
        transaction_amounts = np.random.exponential(50, num_records)
        transaction_types = np.random.choice(['purchase', 'refund', 'subscription', 'addon'], num_records, p=[0.7, 0.1, 0.15, 0.05])
        
        # Geographic data
        countries = np.random.choice(['US', 'UK', 'DE', 'FR', 'JP', 'CA', 'AU'], num_records, p=[0.4, 0.15, 0.1, 0.1, 0.1, 0.1, 0.05])
        
        # Temporal data - last 30 days
        start_date = datetime.now() - timedelta(days=30)
        timestamps = [start_date + timedelta(seconds=x) for x in np.random.randint(0, 30*24*3600, num_records)]
        
        # Status data
        statuses = np.random.choice(['completed', 'pending', 'failed', 'cancelled'], num_records, p=[0.8, 0.1, 0.05, 0.05])
        
        # Create DataFrame
        df = pd.DataFrame({
            'transaction_id': range(1, num_records + 1),
            'user_id': user_ids,
            'timestamp': timestamps,
            'amount': transaction_amounts,
            'transaction_type': transaction_types,
            'country': countries,
            'status': statuses,
            'created_date': [ts.strftime('%Y-%m-%d') for ts in timestamps],
            'processing_time_ms': np.random.normal(100, 30, num_records)
        })
        
        return df
    
    def partition_and_store_data(self, df, base_path='/data/raw/transactions'):
        """
        Partition data by date and store in HDFS for optimal query performance.
        """
        logger.info("Partitioning and storing data in HDFS...")
        
        # Group by date for partitioning
        for date, group in df.groupby('created_date'):
            partition_path = f"{base_path}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}"
            
            # Create directory
            try:
                self.hdfs.makedirs(partition_path)
            except FileExistsError:
                pass
            
            # Convert to various formats for demonstration
            
            # 1. CSV format (human readable)
            csv_data = group.to_csv(index=False)
            with self.hdfs.open(f"{partition_path}/data.csv", 'wb') as f:
                f.write(csv_data.encode('utf-8'))
            
            # 2. JSON format (semi-structured)
            json_data = group.to_json(orient='records', lines=True)
            with self.hdfs.open(f"{partition_path}/data.json", 'wb') as f:
                f.write(json_data.encode('utf-8'))
            
            # 3. Parquet format (optimized for analytics)
            parquet_buffer = group.to_parquet(index=False, engine='pyarrow')
            with self.hdfs.open(f"{partition_path}/data.parquet", 'wb') as f:
                f.write(parquet_buffer)
            
            logger.info(f"Stored {len(group)} records for date {date}")
    
    def create_sample_log_files(self):
        """
        Create sample application log files for MapReduce processing.
        """
        logger.info("Creating sample log files...")
        
        log_path = "/data/logs"
        try:
            self.hdfs.makedirs(log_path)
        except FileExistsError:
            pass
        
        # Generate sample log entries
        log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
        components = ['UserService', 'PaymentProcessor', 'DatabaseConnection', 'CacheManager', 'EmailService']
        
        for day in range(7):  # Last 7 days
            date = (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d')
            log_entries = []
            
            # Generate 10,000 log entries per day
            for i in range(10000):
                timestamp = f"{date} {np.random.randint(0,24):02d}:{np.random.randint(0,60):02d}:{np.random.randint(0,60):02d}"
                level = np.random.choice(log_levels, p=[0.6, 0.2, 0.1, 0.1])
                component = np.random.choice(components)
                
                if level == 'ERROR':
                    messages = [
                        "Connection timeout to database",
                        "Failed to process payment",
                        "User authentication failed",
                        "Cache miss for critical data",
                        "Email delivery failed"
                    ]
                elif level == 'WARN':
                    messages = [
                        "High memory usage detected",
                        "Slow query performance",
                        "Rate limit approaching",
                        "Deprecated API usage"
                    ]
                else:
                    messages = [
                        "Request processed successfully",
                        "User logged in",
                        "Cache updated",
                        "Email sent",
                        "Payment processed"
                    ]
                
                message = np.random.choice(messages)
                log_entry = f"{timestamp} [{level}] {component}: {message} - RequestID:{np.random.randint(100000,999999)}"
                log_entries.append(log_entry)
            
            # Write log file
            log_content = '\n'.join(log_entries)
            with self.hdfs.open(f"{log_path}/app-{date}.log", 'wb') as f:
                f.write(log_content.encode('utf-8'))
            
            logger.info(f"Created log file for {date} with {len(log_entries)} entries")
    
    def create_reference_data(self):
        """
        Create reference/dimension data for analytics.
        """
        logger.info("Creating reference data...")
        
        # User dimension data
        users_df = pd.DataFrame({
            'user_id': range(1, 100001),
            'country': np.random.choice(['US', 'UK', 'DE', 'FR', 'JP', 'CA', 'AU'], 100000),
            'signup_date': pd.date_range('2020-01-01', '2024-01-01', periods=100000),
            'user_type': np.random.choice(['free', 'premium', 'enterprise'], 100000, p=[0.7, 0.25, 0.05]),
            'age_group': np.random.choice(['18-25', '26-35', '36-45', '46-55', '55+'], 100000)
        })
        
        # Store user reference data
        ref_path = "/data/reference/users"
        try:
            self.hdfs.makedirs(ref_path)
        except FileExistsError:
            pass
        
        # Store as Parquet for efficient joins
        parquet_data = users_df.to_parquet(index=False, engine='pyarrow')
        with self.hdfs.open(f"{ref_path}/users.parquet", 'wb') as f:
            f.write(parquet_data)
        
        logger.info(f"Created reference data for {len(users_df)} users")
    
    def run_ingestion_pipeline(self):
        """
        Execute the complete data ingestion pipeline.
        """
        logger.info("Starting Hadoop batch data ingestion pipeline...")
        
        # Generate and ingest transaction data
        transaction_df = self.generate_large_dataset(1000000)  # 1M records
        self.partition_and_store_data(transaction_df)
        
        # Create log files for analysis
        self.create_sample_log_files()
        
        # Create reference data
        self.create_reference_data()
        
        # Verify data in HDFS
        self.verify_data_integrity()
        
        logger.info("Data ingestion pipeline completed successfully!")
    
    def verify_data_integrity(self):
        """
        Verify that data was properly ingested into HDFS.
        """
        logger.info("Verifying data integrity...")
        
        # Check transaction data
        transaction_files = self.hdfs.glob('/data/raw/transactions/year=*/month=*/day=*/*')
        logger.info(f"Found {len(transaction_files)} transaction data files")
        
        # Check log files
        log_files = self.hdfs.glob('/data/logs/*.log')
        logger.info(f"Found {len(log_files)} log files")
        
        # Check reference data
        ref_files = self.hdfs.glob('/data/reference/*/*.parquet')
        logger.info(f"Found {len(ref_files)} reference data files")
        
        # Calculate total size
        total_size = sum(self.hdfs.info(path)['size'] for path in transaction_files + log_files + ref_files)
        logger.info(f"Total data size ingested: {total_size / 1024 / 1024 / 1024:.2f} GB")

if __name__ == "__main__":
    ingestion = HadoopDataIngestion()
    ingestion.run_ingestion_pipeline()
