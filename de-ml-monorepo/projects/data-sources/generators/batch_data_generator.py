#!/usr/bin/env python3
"""
Batch Data Generator for HDFS
Generates large-scale batch data for Hadoop processing demonstrations.
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from hdfs3 import HDFileSystem
import logging
from faker import Faker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode:8020')
DATA_VOLUME_GB = int(os.getenv('DATA_VOLUME_GB', '5'))

fake = Faker()

class BatchDataGenerator:
    def __init__(self):
        try:
            host, port = HDFS_NAMENODE.split(':')
            self.hdfs = HDFileSystem(host=host, port=int(port))
            logger.info(f"Connected to HDFS: {HDFS_NAMENODE}")
        except Exception as e:
            logger.error(f"Failed to connect to HDFS: {e}")
            raise
    
    def generate_ecommerce_data(self, num_records=1000000):
        """Generate e-commerce transaction data."""
        logger.info(f"Generating {num_records:,} e-commerce records...")
        
        np.random.seed(42)
        
        # Generate realistic e-commerce data
        data = {
            'transaction_id': [fake.uuid4() for _ in range(num_records)],
            'user_id': np.random.randint(1, 50000, num_records),
            'product_id': np.random.randint(1, 100000, num_records),
            'timestamp': [
                fake.date_time_between(start_date='-30d', end_date='now')
                for _ in range(num_records)
            ],
            'amount': np.round(np.random.exponential(50, num_records), 2),
            'quantity': np.random.randint(1, 5, num_records),
            'category': np.random.choice([
                'Electronics', 'Clothing', 'Books', 'Home', 'Sports', 
                'Beauty', 'Automotive', 'Food'
            ], num_records),
            'country': np.random.choice([
                'US', 'UK', 'DE', 'FR', 'JP', 'CA', 'AU', 'BR'
            ], num_records, p=[0.3, 0.15, 0.1, 0.1, 0.1, 0.1, 0.05, 0.1]),
            'payment_method': np.random.choice([
                'credit_card', 'debit_card', 'paypal', 'bank_transfer'
            ], num_records, p=[0.5, 0.3, 0.15, 0.05]),
            'status': np.random.choice([
                'completed', 'pending', 'failed', 'cancelled'
            ], num_records, p=[0.85, 0.08, 0.04, 0.03])
        }
        
        df = pd.DataFrame(data)
        df['created_date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
        
        return df
    
    def generate_user_behavior_data(self, num_records=2000000):
        """Generate user behavior/activity data."""
        logger.info(f"Generating {num_records:,} user behavior records...")
        
        activities = [
            'page_view', 'search', 'product_view', 'add_to_cart',
            'remove_from_cart', 'checkout', 'purchase', 'review'
        ]
        
        data = {
            'event_id': [fake.uuid4() for _ in range(num_records)],
            'user_id': np.random.randint(1, 100000, num_records),
            'session_id': [fake.uuid4() for _ in range(num_records)],
            'activity_type': np.random.choice(activities, num_records),
            'timestamp': [
                fake.date_time_between(start_date='-7d', end_date='now')
                for _ in range(num_records)
            ],
            'page_url': [fake.url() for _ in range(num_records)],
            'referrer': np.random.choice([
                'direct', 'google', 'facebook', 'twitter', 'email'
            ], num_records, p=[0.3, 0.4, 0.1, 0.1, 0.1]),
            'device_type': np.random.choice([
                'desktop', 'mobile', 'tablet'
            ], num_records, p=[0.4, 0.5, 0.1]),
            'browser': np.random.choice([
                'chrome', 'firefox', 'safari', 'edge'
            ], num_records, p=[0.6, 0.2, 0.15, 0.05]),
            'duration_seconds': np.random.exponential(120, num_records).astype(int)
        }
        
        return pd.DataFrame(data)
    
    def generate_inventory_data(self, num_products=100000):
        """Generate product inventory data."""
        logger.info(f"Generating {num_products:,} inventory records...")
        
        categories = [
            'Electronics', 'Clothing', 'Books', 'Home & Garden',
            'Sports & Outdoors', 'Beauty & Personal Care', 
            'Automotive', 'Food & Beverages'
        ]
        
        data = {
            'product_id': range(1, num_products + 1),
            'sku': [fake.bothify('SKU-????-####') for _ in range(num_products)],
            'name': [fake.catch_phrase() for _ in range(num_products)],
            'category': np.random.choice(categories, num_products),
            'brand': [fake.company() for _ in range(num_products)],
            'price': np.round(np.random.uniform(5, 500, num_products), 2),
            'cost': np.round(np.random.uniform(2, 300, num_products), 2),
            'stock_quantity': np.random.randint(0, 1000, num_products),
            'weight_kg': np.round(np.random.uniform(0.1, 10, num_products), 2),
            'dimensions_cm': [
                f"{np.random.randint(5,50)}x{np.random.randint(5,50)}x{np.random.randint(5,50)}"
                for _ in range(num_products)
            ],
            'supplier_id': np.random.randint(1, 1000, num_products),
            'created_date': [
                fake.date_between(start_date='-2y', end_date='now')
                for _ in range(num_products)
            ],
            'is_active': np.random.choice([True, False], num_products, p=[0.9, 0.1])
        }
        
        return pd.DataFrame(data)
    
    def store_partitioned_data(self, df, base_path, partition_column='created_date'):
        """Store data in HDFS with partitioning."""
        logger.info(f"Storing data to {base_path} with partitioning...")
        
        try:
            self.hdfs.makedirs(base_path)
        except FileExistsError:
            pass
        
        if partition_column in df.columns:
            # Partition by date
            for partition_value, group in df.groupby(partition_column):
                partition_path = f"{base_path}/date={partition_value}"
                
                try:
                    self.hdfs.makedirs(partition_path)
                except FileExistsError:
                    pass
                
                # Store as both JSON and Parquet
                self._store_formats(group, partition_path)
                
                logger.info(f"Stored {len(group):,} records for {partition_column}={partition_value}")
        else:
            # No partitioning
            self._store_formats(df, base_path)
            logger.info(f"Stored {len(df):,} records without partitioning")
    
    def _store_formats(self, df, path):
        """Store data in multiple formats."""
        # JSON Lines format
        json_data = df.to_json(orient='records', lines=True)
        with self.hdfs.open(f"{path}/data.json", 'wb') as f:
            f.write(json_data.encode('utf-8'))
        
        # CSV format
        csv_data = df.to_csv(index=False)
        with self.hdfs.open(f"{path}/data.csv", 'wb') as f:
            f.write(csv_data.encode('utf-8'))
        
        # Parquet format (most efficient for analytics)
        try:
            parquet_data = df.to_parquet(index=False, engine='pyarrow')
            with self.hdfs.open(f"{path}/data.parquet", 'wb') as f:
                f.write(parquet_data)
        except Exception as e:
            logger.warning(f"Failed to store Parquet format: {e}")
    
    def generate_financial_data(self, num_records=500000):
        """Generate financial/market data."""
        logger.info(f"Generating {num_records:,} financial records...")
        
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NFLX', 'NVDA']
        
        records = []
        for _ in range(num_records):
            symbol = np.random.choice(symbols)
            base_price = np.random.uniform(50, 300)
            
            record = {
                'symbol': symbol,
                'timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
                'open_price': round(base_price + np.random.uniform(-2, 2), 2),
                'high_price': round(base_price + np.random.uniform(0, 5), 2),
                'low_price': round(base_price - np.random.uniform(0, 3), 2),
                'close_price': round(base_price + np.random.uniform(-1, 1), 2),
                'volume': np.random.randint(100000, 10000000),
                'market_cap': np.random.randint(1000000000, 3000000000000),
                'sector': np.random.choice(['Technology', 'Finance', 'Healthcare', 'Energy'])
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
        return df
    
    def run_batch_generation(self):
        """Run the complete batch data generation pipeline."""
        logger.info("Starting batch data generation pipeline...")
        
        try:
            # Calculate records per dataset based on target volume
            target_size_bytes = DATA_VOLUME_GB * 1024 * 1024 * 1024
            
            # Estimate ~1KB per e-commerce record
            ecommerce_records = min(1000000, target_size_bytes // (1024 * 4))
            behavior_records = min(2000000, target_size_bytes // (1024 * 2))
            inventory_records = min(100000, target_size_bytes // (1024 * 8))
            financial_records = min(500000, target_size_bytes // (1024 * 3))
            
            logger.info(f"Target volume: {DATA_VOLUME_GB} GB")
            logger.info(f"Planned records - E-commerce: {ecommerce_records:,}, "
                       f"Behavior: {behavior_records:,}, Inventory: {inventory_records:,}, "
                       f"Financial: {financial_records:,}")
            
            # Generate and store e-commerce data
            ecommerce_df = self.generate_ecommerce_data(ecommerce_records)
            self.store_partitioned_data(ecommerce_df, '/data/batch/ecommerce', 'created_date')
            
            # Generate and store user behavior data
            behavior_df = self.generate_user_behavior_data(behavior_records)
            behavior_df['date'] = behavior_df['timestamp'].dt.strftime('%Y-%m-%d')
            self.store_partitioned_data(behavior_df, '/data/batch/user_behavior', 'date')
            
            # Generate and store inventory data (no partitioning needed)
            inventory_df = self.generate_inventory_data(inventory_records)
            self.store_partitioned_data(inventory_df, '/data/batch/inventory')
            
            # Generate and store financial data
            financial_df = self.generate_financial_data(financial_records)
            self.store_partitioned_data(financial_df, '/data/batch/financial', 'date')
            
            # Summary
            total_records = (len(ecommerce_df) + len(behavior_df) + 
                           len(inventory_df) + len(financial_df))
            logger.info(f"✓ Batch data generation completed!")
            logger.info(f"Total records generated: {total_records:,}")
            
            # Show HDFS directory structure
            self._show_hdfs_summary()
            
        except Exception as e:
            logger.error(f"Batch generation failed: {e}")
            raise
    
    def _show_hdfs_summary(self):
        """Show summary of generated data in HDFS."""
        logger.info("HDFS Data Summary:")
        logger.info("==================")
        
        try:
            batch_files = self.hdfs.glob('/data/batch/**/*')
            total_size = sum(self.hdfs.info(f)['size'] for f in batch_files if self.hdfs.info(f)['kind'] == 'file')
            
            logger.info(f"Total files: {len([f for f in batch_files if self.hdfs.info(f)['kind'] == 'file'])}")
            logger.info(f"Total size: {total_size / 1024 / 1024 / 1024:.2f} GB")
            
            # Show directory breakdown
            for dataset in ['ecommerce', 'user_behavior', 'inventory', 'financial']:
                try:
                    dataset_files = self.hdfs.glob(f'/data/batch/{dataset}/**/*')
                    dataset_size = sum(
                        self.hdfs.info(f)['size'] for f in dataset_files 
                        if self.hdfs.info(f)['kind'] == 'file'
                    )
                    logger.info(f"  {dataset}: {dataset_size / 1024 / 1024:.1f} MB")
                except Exception:
                    pass
                    
        except Exception as e:
            logger.warning(f"Could not generate HDFS summary: {e}")

if __name__ == "__main__":
    generator = BatchDataGenerator()
    generator.run_batch_generation()
