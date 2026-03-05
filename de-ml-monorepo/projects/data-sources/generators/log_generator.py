#!/usr/bin/env python3
"""
Log Files Generator for HDFS
Generates realistic application log files for log analysis demonstrations.
"""

import os
import time
import random
import logging
from datetime import datetime, timedelta
from hdfs3 import HDFileSystem
from faker import Faker
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode:8020')
DATA_RATE = int(os.getenv('BIG_DATA_RATE', '100'))

fake = Faker()

class LogGenerator:
    def __init__(self):
        try:
            host, port = HDFS_NAMENODE.split(':')
            self.hdfs = HDFileSystem(host=host, port=int(port))
            logger.info(f"Connected to HDFS: {HDFS_NAMENODE}")
        except Exception as e:
            logger.error(f"Failed to connect to HDFS: {e}")
            raise
        
        self.components = [
            'UserService', 'PaymentProcessor', 'DatabaseConnection', 
            'CacheManager', 'EmailService', 'AuthenticationService',
            'SearchEngine', 'RecommendationEngine', 'NotificationService',
            'FileUploadService', 'ImageProcessingService', 'AnalyticsService'
        ]
        
        self.log_patterns = {
            'INFO': [
                "Request processed successfully",
                "User logged in successfully", 
                "Cache updated successfully",
                "Email sent successfully",
                "Payment processed successfully",
                "File uploaded successfully",
                "Search query executed",
                "Recommendation generated",
                "Database connection established",
                "Service started successfully"
            ],
            'WARN': [
                "High memory usage detected: {}%",
                "Slow query performance: {} ms",
                "Rate limit approaching for user {}",
                "Deprecated API usage detected",
                "Cache miss rate high: {}%",
                "Connection pool nearly exhausted",
                "Disk space warning: {}% full",
                "High CPU usage: {}%",
                "Network latency high: {} ms",
                "Queue backlog growing: {} items"
            ],
            'ERROR': [
                "Connection timeout to database",
                "Failed to process payment: {}",
                "User authentication failed for user {}",
                "Email delivery failed to {}",
                "File upload failed: {}",
                "Search service unavailable",
                "Cache server unreachable",
                "Database query failed: {}",
                "External API call failed: {}",
                "Service unavailable: {}"
            ],
            'DEBUG': [
                "Processing request: {}",
                "Database query: {}",
                "Cache lookup for key: {}",
                "API call to: {}",
                "User action: {}",
                "System state: {}",
                "Configuration loaded: {}",
                "Memory usage: {} MB",
                "Response time: {} ms",
                "Thread pool size: {}"
            ]
        }
    
    def generate_log_entry(self, timestamp=None):
        """Generate a single realistic log entry."""
        if timestamp is None:
            timestamp = datetime.now()
        
        # Select log level with realistic distribution
        level = np.random.choice(
            ['INFO', 'WARN', 'ERROR', 'DEBUG'],
            p=[0.6, 0.2, 0.1, 0.1]  # Most logs are INFO
        )
        
        component = random.choice(self.components)
        pattern = random.choice(self.log_patterns[level])
        
        # Add realistic context based on level
        if level == 'ERROR':
            context_values = [
                fake.word(),
                fake.user_name(),
                fake.email(),
                fake.file_name(),
                fake.url()
            ]
        elif level == 'WARN':
            context_values = [
                str(random.randint(70, 95)),  # percentages
                str(random.randint(1000, 5000)),  # milliseconds
                fake.user_name(),
                str(random.randint(80, 95))
            ]
        elif level == 'DEBUG':
            context_values = [
                fake.uuid4(),
                f"SELECT * FROM {fake.word()} WHERE id = {random.randint(1, 1000)}",
                fake.word(),
                fake.url(),
                f"{fake.word()}_{fake.word()}",
                fake.word(),
                fake.file_path(),
                str(random.randint(100, 2048)),
                str(random.randint(10, 500))
            ]
        else:  # INFO
            context_values = []
        
        # Format the message
        try:
            if '{}' in pattern and context_values:
                message = pattern.format(random.choice(context_values))
            else:
                message = pattern
        except:
            message = pattern
        
        request_id = fake.uuid4()[:8]
        
        # Format: timestamp [LEVEL] Component: message - RequestID:xxxxx
        log_entry = f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} [{level}] {component}: {message} - RequestID:{request_id}"
        
        return log_entry
    
    def generate_application_logs(self, duration_minutes=60):
        """Generate continuous application logs."""
        logger.info(f"Generating application logs for {duration_minutes} minutes...")
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        current_time = start_time
        
        # Create log directory
        log_path = "/data/logs/applications"
        try:
            self.hdfs.makedirs(log_path)
        except FileExistsError:
            pass
        
        logs_buffer = []
        file_counter = 0
        
        while current_time < end_time:
            # Generate logs for this minute
            minute_logs = []
            
            # Generate realistic log volume (50-200 logs per minute)
            logs_per_minute = random.randint(50, 200)
            
            for _ in range(logs_per_minute):
                # Add some randomness to timestamps within the minute
                log_time = current_time + timedelta(
                    seconds=random.randint(0, 59),
                    microseconds=random.randint(0, 999999)
                )
                log_entry = self.generate_log_entry(log_time)
                minute_logs.append(log_entry)
            
            # Sort logs by timestamp
            minute_logs.sort()
            logs_buffer.extend(minute_logs)
            
            # Write to file every 10 minutes or 10k logs
            if (len(logs_buffer) >= 10000 or 
                (current_time.minute % 10 == 0 and len(logs_buffer) > 0)):
                
                filename = f"app-{current_time.strftime('%Y%m%d-%H%M')}-{file_counter:03d}.log"
                self._write_log_file(logs_buffer, f"{log_path}/{filename}")
                logs_buffer = []
                file_counter += 1
            
            current_time += timedelta(minutes=1)
            
            # Small delay to simulate real-time generation
            if random.random() < 0.1:  # 10% chance
                time.sleep(0.1)
        
        # Write remaining logs
        if logs_buffer:
            filename = f"app-{current_time.strftime('%Y%m%d-%H%M')}-{file_counter:03d}.log"
            self._write_log_file(logs_buffer, f"{log_path}/{filename}")
        
        logger.info("Application log generation completed")
    
    def generate_access_logs(self, duration_minutes=60):
        """Generate web server access logs."""
        logger.info(f"Generating access logs for {duration_minutes} minutes...")
        
        # Common endpoints
        endpoints = [
            '/api/users', '/api/products', '/api/orders', '/api/search',
            '/login', '/logout', '/register', '/dashboard', '/profile',
            '/api/payments', '/api/analytics', '/health', '/metrics'
        ]
        
        # HTTP methods and status codes
        methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
        status_codes = [200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503]
        status_weights = [0.7, 0.1, 0.05, 0.02, 0.02, 0.03, 0.02, 0.01, 0.02, 0.02, 0.005, 0.005]
        
        access_path = "/data/logs/access"
        try:
            self.hdfs.makedirs(access_path)
        except FileExistsError:
            pass
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        current_time = start_time
        
        logs_buffer = []
        
        while current_time < end_time:
            # Generate 100-500 access logs per minute
            logs_per_minute = random.randint(100, 500)
            
            for _ in range(logs_per_minute):
                # Random timestamp within the minute
                log_time = current_time + timedelta(
                    seconds=random.randint(0, 59),
                    microseconds=random.randint(0, 999999)
                )
                
                # Generate access log entry
                ip = fake.ipv4()
                method = np.random.choice(methods, p=[0.7, 0.15, 0.08, 0.05, 0.02])
                endpoint = random.choice(endpoints)
                status = np.random.choice(status_codes, p=status_weights)
                response_size = random.randint(100, 50000)
                response_time = random.randint(10, 2000)  # milliseconds
                user_agent = fake.user_agent()
                
                # Apache/Nginx combined log format
                access_entry = (
                    f'{ip} - - [{log_time.strftime("%d/%b/%Y:%H:%M:%S %z")}] '
                    f'"{method} {endpoint} HTTP/1.1" {status} {response_size} '
                    f'"-" "{user_agent}" {response_time}ms'
                )
                
                logs_buffer.append(access_entry)
            
            current_time += timedelta(minutes=1)
        
        # Write access logs
        filename = f"access-{start_time.strftime('%Y%m%d')}.log"
        self._write_log_file(logs_buffer, f"{access_path}/{filename}")
        
        logger.info("Access log generation completed")
    
    def generate_error_logs(self, duration_minutes=60):
        """Generate dedicated error logs with stack traces."""
        logger.info(f"Generating error logs for {duration_minutes} minutes...")
        
        error_types = [
            'NullPointerException', 'SQLException', 'ConnectionTimeoutException',
            'OutOfMemoryError', 'IllegalArgumentException', 'SecurityException',
            'FileNotFoundException', 'NetworkException', 'ValidationException'
        ]
        
        error_path = "/data/logs/errors"
        try:
            self.hdfs.makedirs(error_path)
        except FileExistsError:
            pass
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        current_time = start_time
        
        error_logs = []
        
        while current_time < end_time:
            # Generate 5-20 errors per minute (realistic error rate)
            errors_per_minute = random.randint(5, 20)
            
            for _ in range(errors_per_minute):
                log_time = current_time + timedelta(
                    seconds=random.randint(0, 59)
                )
                
                error_type = random.choice(error_types)
                component = random.choice(self.components)
                
                # Generate stack trace
                stack_trace = self._generate_stack_trace(error_type)
                
                error_entry = (
                    f"{log_time.strftime('%Y-%m-%d %H:%M:%S')} [ERROR] {component}: "
                    f"{error_type}: {fake.sentence()}\n{stack_trace}\n"
                )
                
                error_logs.append(error_entry)
            
            current_time += timedelta(minutes=1)
        
        # Write error logs
        filename = f"error-{start_time.strftime('%Y%m%d')}.log"
        self._write_log_file(error_logs, f"{error_path}/{filename}")
        
        logger.info("Error log generation completed")
    
    def _generate_stack_trace(self, error_type):
        """Generate a realistic stack trace."""
        stack_lines = []
        depth = random.randint(5, 15)
        
        for i in range(depth):
            class_name = f"com.company.{fake.word()}.{fake.word().title()}"
            method_name = fake.word()
            line_number = random.randint(1, 500)
            
            stack_lines.append(f"\tat {class_name}.{method_name}({class_name.split('.')[-1]}.java:{line_number})")
        
        return '\n'.join(stack_lines)
    
    def _write_log_file(self, logs, filepath):
        """Write log entries to HDFS file."""
        try:
            content = '\n'.join(logs) + '\n'
            with self.hdfs.open(filepath, 'wb') as f:
                f.write(content.encode('utf-8'))
            
            logger.info(f"Written {len(logs):,} log entries to {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to write log file {filepath}: {e}")
    
    def run_log_generation(self):
        """Run the complete log generation pipeline."""
        logger.info("Starting log file generation...")
        
        try:
            # Generate different types of logs
            duration = 30  # 30 minutes of logs
            
            logger.info("Generating application logs...")
            self.generate_application_logs(duration)
            
            logger.info("Generating access logs...")
            self.generate_access_logs(duration)
            
            logger.info("Generating error logs...")
            self.generate_error_logs(duration)
            
            # Show summary
            self._show_log_summary()
            
            logger.info("✓ Log generation completed successfully!")
            
        except Exception as e:
            logger.error(f"Log generation failed: {e}")
            raise
    
    def _show_log_summary(self):
        """Show summary of generated log files."""
        logger.info("Generated Log Files Summary:")
        logger.info("============================")
        
        try:
            log_types = ['applications', 'access', 'errors']
            total_size = 0
            total_files = 0
            
            for log_type in log_types:
                try:
                    files = self.hdfs.glob(f'/data/logs/{log_type}/*')
                    type_size = sum(self.hdfs.info(f)['size'] for f in files)
                    total_size += type_size
                    total_files += len(files)
                    
                    logger.info(f"  {log_type}: {len(files)} files, {type_size / 1024 / 1024:.1f} MB")
                except Exception:
                    logger.info(f"  {log_type}: No files generated")
            
            logger.info(f"Total: {total_files} files, {total_size / 1024 / 1024:.1f} MB")
            
        except Exception as e:
            logger.warning(f"Could not generate log summary: {e}")

if __name__ == "__main__":
    generator = LogGenerator()
    generator.run_log_generation()
