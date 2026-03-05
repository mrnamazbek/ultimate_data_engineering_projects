#!/usr/bin/env python3
"""
IoT Sensor Data Generator
Generates realistic IoT sensor data for time-series analysis and anomaly detection.
"""

import os
import json
import time
import random
import logging
import numpy as np
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
DATA_RATE = int(os.getenv('BIG_DATA_RATE', '100'))

fake = Faker()

class IoTDataGenerator:
    def __init__(self):
        self.producer = None
        self.connect_to_kafka()
        
        # IoT device configurations
        self.sensor_configs = {
            'temperature': {
                'normal_range': (18.0, 25.0),
                'anomaly_range': (-5.0, 45.0),
                'unit': 'celsius',
                'noise_factor': 0.5
            },
            'humidity': {
                'normal_range': (30.0, 70.0),
                'anomaly_range': (0.0, 100.0),
                'unit': 'percentage',
                'noise_factor': 2.0
            },
            'pressure': {
                'normal_range': (1000.0, 1020.0),
                'anomaly_range': (950.0, 1050.0),
                'unit': 'hPa',
                'noise_factor': 0.5
            },
            'light': {
                'normal_range': (100.0, 800.0),
                'anomaly_range': (0.0, 2000.0),
                'unit': 'lux',
                'noise_factor': 10.0
            },
            'motion': {
                'normal_range': (0, 1),
                'anomaly_range': (0, 1),
                'unit': 'binary',
                'noise_factor': 0
            },
            'sound': {
                'normal_range': (30.0, 60.0),
                'anomaly_range': (0.0, 120.0),
                'unit': 'dB',
                'noise_factor': 2.0
            },
            'vibration': {
                'normal_range': (0.1, 2.0),
                'anomaly_range': (0.0, 10.0),
                'unit': 'g',
                'noise_factor': 0.1
            },
            'air_quality': {
                'normal_range': (0.0, 50.0),
                'anomaly_range': (0.0, 300.0),
                'unit': 'AQI',
                'noise_factor': 5.0
            }
        }
        
        # Generate device fleet
        self.devices = self._generate_device_fleet(1000)  # 1000 IoT devices
        
    def connect_to_kafka(self):
        """Connect to Kafka with retry logic."""
        for attempt in range(10):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                    key_serializer=lambda x: x.encode('utf-8'),
                    acks='all'
                )
                logger.info(f"Connected to Kafka: {KAFKA_BROKER}")
                return
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt+1} failed: {e}")
                time.sleep(5)
        raise Exception("Failed to connect to Kafka")
    
    def _generate_device_fleet(self, num_devices):
        """Generate a fleet of IoT devices with realistic configurations."""
        devices = []
        
        # Device locations (smart city deployment)
        locations = [
            {'city': 'New York', 'lat': 40.7128, 'lng': -74.0060},
            {'city': 'London', 'lat': 51.5074, 'lng': -0.1278},
            {'city': 'Tokyo', 'lat': 35.6762, 'lng': 139.6503},
            {'city': 'Sydney', 'lat': -33.8688, 'lng': 151.2093},
            {'city': 'Berlin', 'lat': 52.5200, 'lng': 13.4050}
        ]
        
        # Building types
        building_types = [
            'office', 'residential', 'retail', 'industrial', 
            'healthcare', 'education', 'transportation', 'public'
        ]
        
        for i in range(num_devices):
            location = random.choice(locations)
            building = random.choice(building_types)
            sensor_type = random.choice(list(self.sensor_configs.keys()))
            
            device = {
                'device_id': f"iot_device_{i+1:06d}",
                'sensor_type': sensor_type,
                'location': location,
                'building_type': building,
                'floor': random.randint(1, 20),
                'room': f"Room_{random.randint(100, 999)}",
                'installation_date': fake.date_between(start_date='-2y', end_date='-1m'),
                'firmware_version': f"v{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
                'manufacturer': random.choice(['SensorTech', 'IoTCorp', 'SmartDevices', 'TechSense']),
                'last_maintenance': fake.date_between(start_date='-6m', end_date='now'),
                'battery_level': random.uniform(20.0, 100.0),
                'signal_strength': random.randint(-80, -30),  # dBm
                'is_active': random.choice([True, True, True, False])  # 75% active
            }
            
            devices.append(device)
        
        logger.info(f"Generated {len(devices)} IoT devices")
        return devices
    
    def generate_sensor_reading(self, device):
        """Generate a realistic sensor reading for a device."""
        sensor_type = device['sensor_type']
        config = self.sensor_configs[sensor_type]
        
        # Base reading generation
        if random.random() < 0.05:  # 5% anomalies
            # Generate anomaly
            min_val, max_val = config['anomaly_range']
            base_value = random.uniform(min_val, max_val)
            is_anomaly = True
        else:
            # Generate normal reading
            min_val, max_val = config['normal_range']
            base_value = random.uniform(min_val, max_val)
            is_anomaly = False
        
        # Add realistic noise
        if config['noise_factor'] > 0:
            noise = random.gauss(0, config['noise_factor'])
            value = base_value + noise
        else:
            value = base_value
        
        # Handle binary sensors
        if sensor_type == 'motion':
            value = 1 if random.random() < 0.1 else 0  # 10% motion detection
        
        # Round to appropriate precision
        if sensor_type in ['temperature', 'pressure', 'vibration']:
            value = round(value, 2)
        elif sensor_type in ['humidity', 'sound', 'air_quality']:
            value = round(value, 1)
        elif sensor_type == 'light':
            value = max(0, round(value))
        
        return value, is_anomaly
    
    def generate_device_status(self, device):
        """Generate device health and status information."""
        # Simulate device degradation over time
        days_since_maintenance = (datetime.now().date() - device['last_maintenance']).days
        degradation_factor = min(days_since_maintenance / 180, 1.0)  # Degrade over 6 months
        
        # Battery drain (if battery powered)
        battery_drain_rate = 0.1 * degradation_factor
        device['battery_level'] = max(0, device['battery_level'] - battery_drain_rate)
        
        # Signal strength variation
        base_signal = device['signal_strength']
        signal_variation = random.randint(-10, 10)
        current_signal = max(-100, min(-30, base_signal + signal_variation))
        
        # Device health score
        health_score = (
            (device['battery_level'] / 100.0) * 0.4 +
            ((current_signal + 100) / 70.0) * 0.3 +
            (1 - degradation_factor) * 0.3
        ) * 100
        
        return {
            'battery_level': round(device['battery_level'], 1),
            'signal_strength': current_signal,
            'health_score': round(health_score, 1),
            'days_since_maintenance': days_since_maintenance,
            'status': 'active' if device['is_active'] and health_score > 20 else 'inactive'
        }
    
    def create_iot_event(self, device):
        """Create a complete IoT sensor event."""
        timestamp = datetime.utcnow()
        sensor_value, is_anomaly = self.generate_sensor_reading(device)
        device_status = self.generate_device_status(device)
        
        event = {
            'event_id': fake.uuid4(),
            'device_id': device['device_id'],
            'sensor_type': device['sensor_type'],
            'timestamp': timestamp,
            
            # Sensor reading
            'value': sensor_value,
            'unit': self.sensor_configs[device['sensor_type']]['unit'],
            'is_anomaly': is_anomaly,
            
            # Location information
            'location': {
                'city': device['location']['city'],
                'latitude': device['location']['lat'],
                'longitude': device['location']['lng'],
                'building_type': device['building_type'],
                'floor': device['floor'],
                'room': device['room']
            },
            
            # Device information
            'device_info': {
                'manufacturer': device['manufacturer'],
                'firmware_version': device['firmware_version'],
                'installation_date': device['installation_date'],
                'battery_level': device_status['battery_level'],
                'signal_strength': device_status['signal_strength'],
                'health_score': device_status['health_score'],
                'status': device_status['status']
            },
            
            # Environmental context
            'environmental': self._generate_environmental_context(device),
            
            # Data quality metrics
            'data_quality': {
                'accuracy': random.uniform(95.0, 99.9),
                'completeness': random.uniform(98.0, 100.0),
                'timeliness': random.uniform(0.1, 2.0)  # seconds delay
            }
        }
        
        return event
    
    def _generate_environmental_context(self, device):
        """Generate environmental context that might affect sensor readings."""
        # Simulate seasonal and daily patterns
        now = datetime.now()
        hour = now.hour
        month = now.month
        
        # Daily patterns
        if 6 <= hour <= 18:  # Daytime
            activity_level = 'high'
            external_light = random.uniform(200, 1000)
        else:  # Nighttime
            activity_level = 'low'
            external_light = random.uniform(0, 50)
        
        # Seasonal patterns (Northern hemisphere)
        if month in [12, 1, 2]:  # Winter
            season = 'winter'
            base_temp = random.uniform(-5, 10)
        elif month in [3, 4, 5]:  # Spring
            season = 'spring'
            base_temp = random.uniform(10, 20)
        elif month in [6, 7, 8]:  # Summer
            season = 'summer'
            base_temp = random.uniform(20, 35)
        else:  # Fall
            season = 'fall'
            base_temp = random.uniform(5, 20)
        
        return {
            'season': season,
            'hour_of_day': hour,
            'activity_level': activity_level,
            'external_temperature': round(base_temp, 1),
            'external_light': round(external_light),
            'weather_condition': random.choice([
                'sunny', 'cloudy', 'rainy', 'snowy', 'foggy'
            ])
        }
    
    def run_iot_generation(self):
        """Run continuous IoT data generation."""
        logger.info(f"Starting IoT data generation with {len(self.devices)} devices")
        logger.info(f"Target rate: {DATA_RATE} events/second")
        
        events_sent = 0
        start_time = time.time()
        
        try:
            while True:
                batch_start = time.time()
                
                # Generate batch of events
                for _ in range(DATA_RATE):
                    # Select random active device
                    active_devices = [d for d in self.devices if d['is_active']]
                    if not active_devices:
                        continue
                    
                    device = random.choice(active_devices)
                    event = self.create_iot_event(device)
                    
                    # Send to Kafka
                    try:
                        self.producer.send(
                            'iot_sensors',
                            value=event,
                            key=event['device_id']
                        )
                        events_sent += 1
                    except Exception as e:
                        logger.error(f"Failed to send IoT event: {e}")
                
                # Flush producer periodically
                if events_sent % 1000 == 0:
                    self.producer.flush()
                
                # Log progress
                if events_sent % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = events_sent / elapsed
                    logger.info(f"Sent {events_sent:,} IoT events ({rate:.1f} events/sec)")
                
                # Maintain rate
                batch_duration = time.time() - batch_start
                sleep_time = max(0, 1.0 - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("IoT generator stopped")
        except Exception as e:
            logger.error(f"IoT generator error: {e}")
        finally:
            if self.producer:
                self.producer.close()
            
            elapsed = time.time() - start_time
            logger.info(f"Generated {events_sent:,} IoT events in {elapsed:.1f} seconds")

if __name__ == "__main__":
    generator = IoTDataGenerator()
    generator.run_iot_generation()
