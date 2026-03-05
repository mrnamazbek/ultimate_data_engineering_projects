#!/usr/bin/env python3
"""
Progress Monitor for Big Data Sources Generator
Monitors data generation progress and provides real-time statistics.
"""

import os
import time
import json
import logging
from datetime import datetime, timedelta
from kafka import KafkaConsumer, TopicPartition
from hdfs3 import HDFileSystem

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode:8020')
DATA_RATE = int(os.getenv('BIG_DATA_RATE', '100'))
TARGET_VOLUME_GB = int(os.getenv('DATA_VOLUME_GB', '10'))

class DataGenerationMonitor:
    def __init__(self):
        self.start_time = datetime.now()
        self.kafka_broker = KAFKA_BROKER
        self.hdfs_namenode = HDFS_NAMENODE
        
        # Initialize connections
        self.setup_kafka_consumer()
        self.setup_hdfs_connection()
        
        # Statistics tracking
        self.stats = {
            'kafka_events': 0,
            'hdfs_files': 0,
            'hdfs_size_gb': 0.0,
            'last_update': datetime.now(),
            'topics': {}
        }
        
        # Kafka topics to monitor
        self.kafka_topics = [
            'events', 'user_activity', 'iot_sensors', 
            'social_media', 'market_stock', 'market_crypto', 
            'market_forex', 'market_indices'
        ]
    
    def setup_kafka_consumer(self):
        """Setup Kafka consumer for monitoring."""
        try:
            self.kafka_consumer = KafkaConsumer(
                bootstrap_servers=[self.kafka_broker],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )
            logger.info("Connected to Kafka for monitoring")
        except Exception as e:
            logger.warning(f"Could not connect to Kafka: {e}")
            self.kafka_consumer = None
    
    def setup_hdfs_connection(self):
        """Setup HDFS connection for monitoring."""
        try:
            host, port = self.hdfs_namenode.split(':')
            self.hdfs = HDFileSystem(host=host, port=int(port))
            logger.info("Connected to HDFS for monitoring")
        except Exception as e:
            logger.warning(f"Could not connect to HDFS: {e}")
            self.hdfs = None
    
    def get_kafka_statistics(self):
        """Get Kafka topic statistics."""
        if not self.kafka_consumer:
            return {}
        
        topic_stats = {}
        
        try:
            # Get metadata for all topics
            metadata = self.kafka_consumer.list_consumer_groups()
            
            for topic in self.kafka_topics:
                try:
                    partitions = self.kafka_consumer.partitions_for_topic(topic)
                    if partitions:
                        total_messages = 0
                        
                        # Get high water marks for all partitions
                        topic_partitions = [TopicPartition(topic, p) for p in partitions]
                        end_offsets = self.kafka_consumer.end_offsets(topic_partitions)
                        beginning_offsets = self.kafka_consumer.beginning_offsets(topic_partitions)
                        
                        for tp in topic_partitions:
                            messages_in_partition = end_offsets[tp] - beginning_offsets[tp]
                            total_messages += messages_in_partition
                        
                        topic_stats[topic] = {
                            'partitions': len(partitions),
                            'messages': total_messages,
                            'size_mb': total_messages * 0.001  # Rough estimate
                        }
                
                except Exception as e:
                    logger.debug(f"Could not get stats for topic {topic}: {e}")
                    topic_stats[topic] = {'partitions': 0, 'messages': 0, 'size_mb': 0}
        
        except Exception as e:
            logger.warning(f"Error getting Kafka statistics: {e}")
        
        return topic_stats
    
    def get_hdfs_statistics(self):
        """Get HDFS storage statistics."""
        if not self.hdfs:
            return {'files': 0, 'size_gb': 0.0, 'directories': []}
        
        try:
            # Check data directories
            data_paths = [
                '/data/batch',
                '/data/logs', 
                '/data/raw',
                '/data/processed'
            ]
            
            total_files = 0
            total_size = 0
            directories = []
            
            for path in data_paths:
                try:
                    if self.hdfs.exists(path):
                        files = self.hdfs.glob(f'{path}/**/*')
                        path_files = 0
                        path_size = 0
                        
                        for file in files:
                            try:
                                info = self.hdfs.info(file)
                                if info['kind'] == 'file':
                                    path_files += 1
                                    path_size += info['size']
                            except:
                                continue
                        
                        total_files += path_files
                        total_size += path_size
                        
                        directories.append({
                            'path': path,
                            'files': path_files,
                            'size_mb': path_size / 1024 / 1024
                        })
                
                except Exception as e:
                    logger.debug(f"Could not analyze path {path}: {e}")
            
            return {
                'files': total_files,
                'size_gb': total_size / 1024 / 1024 / 1024,
                'directories': directories
            }
        
        except Exception as e:
            logger.warning(f"Error getting HDFS statistics: {e}")
            return {'files': 0, 'size_gb': 0.0, 'directories': []}
    
    def calculate_progress(self):
        """Calculate overall progress towards target."""
        elapsed_time = datetime.now() - self.start_time
        elapsed_minutes = elapsed_time.total_seconds() / 60
        
        # Get current statistics
        kafka_stats = self.get_kafka_statistics()
        hdfs_stats = self.get_hdfs_statistics()
        
        # Calculate totals
        total_kafka_messages = sum(topic.get('messages', 0) for topic in kafka_stats.values())
        total_kafka_size_mb = sum(topic.get('size_mb', 0) for topic in kafka_stats.values())
        
        total_hdfs_size_gb = hdfs_stats['size_gb']
        total_size_gb = (total_kafka_size_mb / 1024) + total_hdfs_size_gb
        
        # Calculate rates
        if elapsed_minutes > 0:
            message_rate = total_kafka_messages / elapsed_minutes / 60  # messages per second
            data_rate_mb_min = (total_size_gb * 1024) / elapsed_minutes  # MB per minute
        else:
            message_rate = 0
            data_rate_mb_min = 0
        
        # Calculate progress percentage
        progress_pct = min(100.0, (total_size_gb / TARGET_VOLUME_GB) * 100) if TARGET_VOLUME_GB > 0 else 0
        
        # Estimate completion time
        if data_rate_mb_min > 0 and progress_pct < 100:
            remaining_gb = TARGET_VOLUME_GB - total_size_gb
            remaining_minutes = (remaining_gb * 1024) / data_rate_mb_min
            eta = datetime.now() + timedelta(minutes=remaining_minutes)
        else:
            eta = None
        
        return {
            'elapsed_minutes': elapsed_minutes,
            'total_messages': total_kafka_messages,
            'total_size_gb': total_size_gb,
            'target_size_gb': TARGET_VOLUME_GB,
            'progress_pct': progress_pct,
            'message_rate': message_rate,
            'data_rate_mb_min': data_rate_mb_min,
            'eta': eta,
            'kafka_stats': kafka_stats,
            'hdfs_stats': hdfs_stats
        }
    
    def display_progress(self, stats):
        """Display formatted progress information."""
        print("\n" + "="*80)
        print(f"🚀 BIG DATA GENERATION PROGRESS REPORT")
        print(f"📅 Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Elapsed: {stats['elapsed_minutes']:.1f} minutes")
        print("="*80)
        
        # Overall Progress
        print(f"📊 OVERALL PROGRESS")
        print(f"   Progress: {stats['progress_pct']:.1f}% ({stats['total_size_gb']:.2f} GB / {stats['target_size_gb']} GB)")
        print(f"   Data Rate: {stats['data_rate_mb_min']:.1f} MB/min")
        print(f"   Message Rate: {stats['message_rate']:.1f} msg/sec")
        
        if stats['eta']:
            print(f"   ETA: {stats['eta'].strftime('%H:%M:%S')}")
        
        # Kafka Statistics
        print(f"\n📡 KAFKA STREAMING DATA")
        print(f"   Total Messages: {stats['total_messages']:,}")
        
        for topic, topic_stats in stats['kafka_stats'].items():
            if topic_stats['messages'] > 0:
                print(f"   • {topic}: {topic_stats['messages']:,} messages ({topic_stats['size_mb']:.1f} MB)")
        
        # HDFS Statistics  
        print(f"\n💾 HDFS BATCH DATA")
        print(f"   Total Files: {stats['hdfs_stats']['files']:,}")
        print(f"   Total Size: {stats['hdfs_stats']['size_gb']:.2f} GB")
        
        for dir_info in stats['hdfs_stats']['directories']:
            if dir_info['files'] > 0:
                print(f"   • {dir_info['path']}: {dir_info['files']} files ({dir_info['size_mb']:.1f} MB)")
        
        # Progress Bar
        progress_bar_width = 50
        filled_width = int(progress_bar_width * stats['progress_pct'] / 100)
        bar = "█" * filled_width + "░" * (progress_bar_width - filled_width)
        print(f"\n📈 Progress: [{bar}] {stats['progress_pct']:.1f}%")
        
        print("="*80)
    
    def run_monitoring(self):
        """Run continuous monitoring loop."""
        logger.info("Starting data generation monitoring...")
        logger.info(f"Target: {TARGET_VOLUME_GB} GB, Rate: {DATA_RATE} events/sec")
        
        try:
            while True:
                # Calculate and display progress
                progress_stats = self.calculate_progress()
                self.display_progress(progress_stats)
                
                # Check if target reached
                if progress_stats['progress_pct'] >= 100:
                    print("\n🎉 TARGET DATA VOLUME REACHED! 🎉")
                    break
                
                # Wait before next update
                time.sleep(30)  # Update every 30 seconds
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
        finally:
            # Final report
            final_stats = self.calculate_progress()
            print(f"\n📋 FINAL REPORT")
            print(f"   Data Generated: {final_stats['total_size_gb']:.2f} GB")
            print(f"   Messages Sent: {final_stats['total_messages']:,}")
            print(f"   Total Runtime: {final_stats['elapsed_minutes']:.1f} minutes")
            print(f"   Completion: {final_stats['progress_pct']:.1f}%")
            
            if self.kafka_consumer:
                self.kafka_consumer.close()

if __name__ == "__main__":
    monitor = DataGenerationMonitor()
    monitor.run_monitoring()
