#!/usr/bin/env python3
"""
Hive Analytics Script for Hadoop Batch Processing
Demonstrates SQL-like analytics on big data using Apache Hive.
"""

import os
import sys
import logging
from pyhive import hive
from sqlalchemy import create_engine
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
HIVE_HOST = 'hive-metastore'
HIVE_PORT = 10000
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode:8020')

class HiveAnalytics:
    def __init__(self):
        self.connection = None
        self.connect_to_hive()
    
    def connect_to_hive(self):
        """
        Establish connection to Hive for SQL analytics.
        """
        try:
            self.connection = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, database='default')
            logger.info(f"Connected to Hive at {HIVE_HOST}:{HIVE_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to Hive: {e}")
            sys.exit(1)
    
    def create_external_tables(self):
        """
        Create external Hive tables pointing to HDFS data.
        """
        logger.info("Creating Hive external tables...")
        
        # Create database for analytics
        create_db_sql = """
        CREATE DATABASE IF NOT EXISTS analytics_db
        COMMENT 'Analytics database for batch processing'
        """
        
        self._execute_sql(create_db_sql)
        
        # Use analytics database
        self._execute_sql("USE analytics_db")
        
        # Create transactions table (partitioned)
        transactions_table_sql = """
        CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
            transaction_id BIGINT,
            user_id INT,
            timestamp TIMESTAMP,
            amount DOUBLE,
            transaction_type STRING,
            country STRING,
            status STRING,
            processing_time_ms DOUBLE
        )
        PARTITIONED BY (
            year STRING,
            month STRING, 
            day STRING
        )
        STORED AS PARQUET
        LOCATION 'hdfs://namenode:8020/data/raw/transactions/'
        TBLPROPERTIES ('parquet.compression'='SNAPPY')
        """
        
        self._execute_sql(transactions_table_sql)
        
        # Create users reference table
        users_table_sql = """
        CREATE EXTERNAL TABLE IF NOT EXISTS users (
            user_id INT,
            country STRING,
            signup_date DATE,
            user_type STRING,
            age_group STRING
        )
        STORED AS PARQUET
        LOCATION 'hdfs://namenode:8020/data/reference/users/'
        """
        
        self._execute_sql(users_table_sql)
        
        # Repair partitions to auto-discover partition directories
        self._execute_sql("MSCK REPAIR TABLE transactions")
        
        logger.info("External tables created successfully")
    
    def run_daily_aggregations(self):
        """
        Run daily transaction aggregations for business metrics.
        """
        logger.info("Running daily aggregations...")
        
        # Daily transaction summary
        daily_summary_sql = """
        CREATE TABLE IF NOT EXISTS daily_transaction_summary
        STORED AS PARQUET
        AS SELECT 
            CONCAT(year, '-', month, '-', day) as transaction_date,
            COUNT(*) as total_transactions,
            COUNT(DISTINCT user_id) as unique_users,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_transaction_amount,
            AVG(processing_time_ms) as avg_processing_time,
            
            -- Transaction type breakdown
            SUM(CASE WHEN transaction_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            SUM(CASE WHEN transaction_type = 'refund' THEN 1 ELSE 0 END) as refunds,
            SUM(CASE WHEN transaction_type = 'subscription' THEN 1 ELSE 0 END) as subscriptions,
            
            -- Status breakdown
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_txns,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_txns,
            
            -- Revenue by status
            SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_revenue
            
        FROM transactions
        WHERE year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
        GROUP BY year, month, day
        ORDER BY transaction_date DESC
        """
        
        self._execute_sql("DROP TABLE IF EXISTS daily_transaction_summary")
        self._execute_sql(daily_summary_sql)
        
        logger.info("Daily aggregations completed")
    
    def run_user_analytics(self):
        """
        Run user-level analytics combining transactions with user dimensions.
        """
        logger.info("Running user analytics...")
        
        # User lifetime value and activity analysis
        user_analytics_sql = """
        CREATE TABLE IF NOT EXISTS user_analytics
        STORED AS PARQUET
        AS SELECT 
            t.user_id,
            u.country,
            u.user_type,
            u.age_group,
            u.signup_date,
            
            -- Transaction metrics
            COUNT(t.transaction_id) as total_transactions,
            SUM(CASE WHEN t.status = 'completed' THEN t.amount ELSE 0 END) as lifetime_value,
            AVG(t.amount) as avg_transaction_amount,
            MAX(t.timestamp) as last_transaction_date,
            MIN(t.timestamp) as first_transaction_date,
            
            -- Activity patterns
            COUNT(DISTINCT CONCAT(year, '-', month, '-', day)) as active_days,
            AVG(t.processing_time_ms) as avg_processing_time,
            
            -- Transaction type preferences
            SUM(CASE WHEN t.transaction_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            SUM(CASE WHEN t.transaction_type = 'subscription' THEN 1 ELSE 0 END) as subscriptions,
            
            -- Success rate
            ROUND(
                SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
                COUNT(t.transaction_id), 2
            ) as success_rate_pct
            
        FROM transactions t
        JOIN users u ON t.user_id = u.user_id
        WHERE t.year IS NOT NULL AND t.month IS NOT NULL AND t.day IS NOT NULL
        GROUP BY 
            t.user_id, u.country, u.user_type, u.age_group, u.signup_date
        HAVING COUNT(t.transaction_id) >= 5  -- Users with at least 5 transactions
        """
        
        self._execute_sql("DROP TABLE IF EXISTS user_analytics")
        self._execute_sql(user_analytics_sql)
        
        logger.info("User analytics completed")
    
    def run_geographic_analysis(self):
        """
        Run geographic analysis for regional insights.
        """
        logger.info("Running geographic analysis...")
        
        geographic_analysis_sql = """
        CREATE TABLE IF NOT EXISTS geographic_analysis
        STORED AS PARQUET
        AS SELECT 
            t.country,
            COUNT(DISTINCT t.user_id) as unique_users,
            COUNT(t.transaction_id) as total_transactions,
            SUM(CASE WHEN t.status = 'completed' THEN t.amount ELSE 0 END) as total_revenue,
            AVG(t.amount) as avg_transaction_amount,
            AVG(t.processing_time_ms) as avg_processing_time,
            
            -- User type distribution
            SUM(CASE WHEN u.user_type = 'premium' THEN 1 ELSE 0 END) as premium_users,
            SUM(CASE WHEN u.user_type = 'enterprise' THEN 1 ELSE 0 END) as enterprise_users,
            
            -- Success metrics
            ROUND(
                SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
                COUNT(t.transaction_id), 2
            ) as success_rate_pct,
            
            -- Revenue per user
            ROUND(
                SUM(CASE WHEN t.status = 'completed' THEN t.amount ELSE 0 END) / 
                COUNT(DISTINCT t.user_id), 2
            ) as revenue_per_user
            
        FROM transactions t
        JOIN users u ON t.user_id = u.user_id
        WHERE t.year IS NOT NULL AND t.month IS NOT NULL AND t.day IS NOT NULL
        GROUP BY t.country
        ORDER BY total_revenue DESC
        """
        
        self._execute_sql("DROP TABLE IF EXISTS geographic_analysis")
        self._execute_sql(geographic_analysis_sql)
        
        logger.info("Geographic analysis completed")
    
    def run_time_series_analysis(self):
        """
        Run time-series analysis for trend identification.
        """
        logger.info("Running time-series analysis...")
        
        time_series_sql = """
        CREATE TABLE IF NOT EXISTS hourly_transaction_trends
        STORED AS PARQUET
        AS SELECT 
            CONCAT(year, '-', month, '-', day) as transaction_date,
            HOUR(timestamp) as hour_of_day,
            COUNT(*) as transaction_count,
            COUNT(DISTINCT user_id) as unique_users,
            SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as hourly_revenue,
            AVG(processing_time_ms) as avg_processing_time,
            
            -- Success rate by hour
            ROUND(
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
                COUNT(*), 2
            ) as success_rate_pct
            
        FROM transactions
        WHERE year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
        GROUP BY year, month, day, HOUR(timestamp)
        ORDER BY transaction_date, hour_of_day
        """
        
        self._execute_sql("DROP TABLE IF EXISTS hourly_transaction_trends")
        self._execute_sql(time_series_sql)
        
        logger.info("Time-series analysis completed")
    
    def create_business_kpi_views(self):
        """
        Create views for key business metrics and KPIs.
        """
        logger.info("Creating business KPI views...")
        
        # Overall business metrics view
        kpi_view_sql = """
        CREATE VIEW IF NOT EXISTS business_kpis AS
        SELECT 
            'overall' as metric_scope,
            COUNT(DISTINCT t.user_id) as total_users,
            COUNT(t.transaction_id) as total_transactions,
            SUM(CASE WHEN t.status = 'completed' THEN t.amount ELSE 0 END) as total_revenue,
            AVG(t.amount) as avg_transaction_value,
            
            -- User segmentation
            COUNT(DISTINCT CASE WHEN u.user_type = 'premium' THEN t.user_id END) as premium_users,
            COUNT(DISTINCT CASE WHEN u.user_type = 'enterprise' THEN t.user_id END) as enterprise_users,
            
            -- Success metrics
            ROUND(
                SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / 
                COUNT(t.transaction_id), 2
            ) as overall_success_rate,
            
            CURRENT_TIMESTAMP() as calculated_at
            
        FROM transactions t
        JOIN users u ON t.user_id = u.user_id
        WHERE t.year IS NOT NULL AND t.month IS NOT NULL AND t.day IS NOT NULL
        """
        
        self._execute_sql("DROP VIEW IF EXISTS business_kpis")
        self._execute_sql(kpi_view_sql)
        
        logger.info("Business KPI views created")
    
    def _execute_sql(self, sql):
        """
        Execute SQL query with error handling.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            logger.debug(f"Executed SQL: {sql[:100]}...")
            cursor.close()
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            logger.error(f"Query: {sql[:200]}...")
            raise
    
    def run_analytics_pipeline(self):
        """
        Execute the complete Hive analytics pipeline.
        """
        logger.info("Starting Hive analytics pipeline...")
        
        try:
            # Create tables
            self.create_external_tables()
            
            # Run analytics
            self.run_daily_aggregations()
            self.run_user_analytics()
            self.run_geographic_analysis()
            self.run_time_series_analysis()
            self.create_business_kpi_views()
            
            # Show sample results
            self.show_sample_results()
            
            logger.info("Hive analytics pipeline completed successfully!")
            
        except Exception as e:
            logger.error(f"Analytics pipeline failed: {e}")
            raise
        finally:
            if self.connection:
                self.connection.close()
    
    def show_sample_results(self):
        """
        Display sample results from analytics tables.
        """
        logger.info("Showing sample analytics results...")
        
        sample_queries = [
            ("Daily Summary Sample", "SELECT * FROM daily_transaction_summary LIMIT 5"),
            ("Geographic Analysis Sample", "SELECT * FROM geographic_analysis LIMIT 5"),
            ("Business KPIs", "SELECT * FROM business_kpis")
        ]
        
        cursor = self.connection.cursor()
        
        for description, query in sample_queries:
            logger.info(f"\n=== {description} ===")
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                for row in results[:3]:  # Show first 3 rows
                    logger.info(f"  {row}")
            except Exception as e:
                logger.warning(f"Sample query failed: {e}")
        
        cursor.close()

if __name__ == "__main__":
    analytics = HiveAnalytics()
    analytics.run_analytics_pipeline()
