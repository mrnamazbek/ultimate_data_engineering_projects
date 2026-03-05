#!/usr/bin/env python3
"""
Streaming Data Generator for Kafka
Generates realistic e-commerce and user activity data for real-time processing.
"""

import os
import json
import time
import random
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import numpy as np

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
DATA_RATE = int(os.getenv('BIG_DATA_RATE', '100'))

fake = Faker()

class StreamingDataGenerator:
    def __init__(self):
        self.producer = None
        self.connect_to_kafka()
        self.user_sessions = {}  # Track active user sessions
        self.products = self.generate_product_catalog()
        
    def connect_to_kafka(self):
        """Connect to Kafka with retry logic."""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                    key_serializer=lambda x: x.encode('utf-8') if x else None,
                    acks='all',
                    retries=3
                )
                logger.info(f"Connected to Kafka: {KAFKA_BROKER}")
                return
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt+1} failed: {e}")
                time.sleep(5)
        
        raise Exception("Failed to connect to Kafka after all retries")
    
    def generate_product_catalog(self):
        """Generate a realistic product catalog."""
        categories = [
            'Electronics', 'Clothing', 'Books', 'Home & Garden', 
            'Sports', 'Beauty', 'Automotive', 'Food'
        ]
        
        products = []
        for i in range(10000):  # 10k products
            product = {
                'product_id': i + 1,
                'name': fake.catch_phrase(),
                'category': random.choice(categories),
                'price': round(random.uniform(5.0, 500.0), 2),
                'rating': round(random.uniform(1.0, 5.0), 1),
                'in_stock': random.choice([True, True, True, False]),  # 75% in stock
                'brand': fake.company()
            }
            products.append(product)
        
        logger.info(f"Generated product catalog with {len(products)} products")
        return products
    
    def generate_user_event(self):
        """Generate a realistic user activity event."""
        event_types = [
            'page_view', 'product_view', 'add_to_cart', 'remove_from_cart',
            'checkout_start', 'purchase', 'search', 'login', 'logout',
            'review_write', 'wishlist_add'
        ]
        
        # Select event type with realistic probabilities
        event_type = np.random.choice(event_types, p=[
            0.4, 0.2, 0.1, 0.02, 0.05, 0.08, 0.1, 0.02, 0.01, 0.01, 0.01
        ])
        
        user_id = random.randint(1, 100000)
        session_id = self.get_or_create_session(user_id)
        
        base_event = {
            'event_id': fake.uuid4(),
            'user_id': user_id,
            'session_id': session_id,
            'event_type': event_type,
            'timestamp': datetime.utcnow(),
            'ip_address': fake.ipv4(),
            'user_agent': fake.user_agent(),
            'country': fake.country_code(),
            'device_type': random.choice(['desktop', 'mobile', 'tablet'])
        }
        
        # Add event-specific data
        if event_type in ['product_view', 'add_to_cart', 'remove_from_cart']:
            product = random.choice(self.products)
            base_event.update({
                'product_id': product['product_id'],
                'category': product['category'],
                'price': product['price'],
                'brand': product['brand']
            })
        
        elif event_type == 'purchase':
            # Simulate realistic purchase behavior
            items = random.sample(self.products, random.randint(1, 5))
            base_event.update({
                'transaction_id': fake.uuid4(),
                'items': [
                    {
                        'product_id': item['product_id'],
                        'quantity': random.randint(1, 3),
                        'unit_price': item['price']
                    } for item in items
                ],
                'total_amount': sum(item['price'] * random.randint(1, 3) for item in items),
                'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay'])
            })
        
        elif event_type == 'search':
            base_event.update({
                'search_query': fake.catch_phrase(),
                'results_count': random.randint(0, 1000)
            })
        
        return base_event
    
    def generate_iot_sensor_event(self):
        """Generate IoT sensor data."""
        sensor_types = ['temperature', 'humidity', 'pressure', 'motion', 'light']
        
        event = {
            'sensor_id': f"sensor_{random.randint(1, 1000)}",
            'sensor_type': random.choice(sensor_types),
            'timestamp': datetime.utcnow(),
            'location': {
                'lat': fake.latitude(),
                'lng': fake.longitude(),
                'building': fake.building_number(),
                'room': f"Room {random.randint(101, 999)}"
            }
        }
        
        # Generate realistic sensor readings based on type
        if event['sensor_type'] == 'temperature':
            event['value'] = round(random.uniform(15.0, 30.0), 2)
            event['unit'] = 'celsius'
        elif event['sensor_type'] == 'humidity':
            event['value'] = round(random.uniform(30.0, 80.0), 2)
            event['unit'] = 'percentage'
        elif event['sensor_type'] == 'pressure':
            event['value'] = round(random.uniform(980.0, 1020.0), 2)
            event['unit'] = 'hPa'
        elif event['sensor_type'] == 'motion':
            event['value'] = random.choice([0, 1])
            event['unit'] = 'binary'
        elif event['sensor_type'] == 'light':
            event['value'] = round(random.uniform(0.0, 1000.0), 2)
            event['unit'] = 'lux'
        
        # Add realistic anomalies (5% chance)
        if random.random() < 0.05:
            event['anomaly'] = True
            if event['sensor_type'] == 'temperature':
                event['value'] = round(random.uniform(-10.0, 50.0), 2)
            elif event['sensor_type'] == 'humidity':
                event['value'] = round(random.uniform(0.0, 100.0), 2)
        
        return event
    
    def generate_social_media_event(self):
        """Generate social media activity data."""
        event_types = ['post', 'like', 'share', 'comment', 'follow', 'unfollow']
        
        event = {
            'event_id': fake.uuid4(),
            'user_id': random.randint(1, 1000000),
            'event_type': random.choice(event_types),
            'timestamp': datetime.utcnow(),
            'platform': random.choice(['twitter', 'facebook', 'instagram', 'linkedin']),
            'content_type': random.choice(['text', 'image', 'video', 'link'])
        }
        
        if event['event_type'] == 'post':
            event.update({
                'content_id': fake.uuid4(),
                'content_length': random.randint(10, 280),
                'hashtags': [f"#{fake.word()}" for _ in range(random.randint(0, 5))],
                'mentions': random.randint(0, 3)
            })
        elif event['event_type'] in ['like', 'share', 'comment']:
            event.update({
                'target_content_id': fake.uuid4(),
                'target_user_id': random.randint(1, 1000000)
            })
        
        return event
    
    def get_or_create_session(self, user_id):
        """Manage user sessions realistically."""
        now = datetime.utcnow()
        
        # Clean up old sessions
        expired_sessions = [
            uid for uid, session in self.user_sessions.items()
            if now - session['start_time'] > timedelta(hours=2)
        ]
        for uid in expired_sessions:
            del self.user_sessions[uid]
        
        # Get or create session
        if user_id not in self.user_sessions or random.random() < 0.1:  # 10% chance new session
            self.user_sessions[user_id] = {
                'session_id': fake.uuid4(),
                'start_time': now
            }
        
        return self.user_sessions[user_id]['session_id']
    
    def send_to_kafka_topics(self, events):
        """Send events to appropriate Kafka topics."""
        topic_mapping = {
            'user_activity': ['page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 
                             'checkout_start', 'purchase', 'search', 'login', 'logout', 
                             'review_write', 'wishlist_add'],
            'iot_sensors': ['temperature', 'humidity', 'pressure', 'motion', 'light'],
            'social_media': ['post', 'like', 'share', 'comment', 'follow', 'unfollow'],
            'events': []  # Default topic for compatibility
        }
        
        for event in events:
            # Determine topic
            event_type = event.get('event_type', event.get('sensor_type', 'unknown'))
            topic = 'events'  # Default
            
            for topic_name, types in topic_mapping.items():
                if event_type in types:
                    topic = topic_name
                    break
            
            # Send to Kafka
            try:
                self.producer.send(
                    topic,
                    value=event,
                    key=str(event.get('user_id', event.get('sensor_id', 'unknown')))
                )
            except Exception as e:
                logger.error(f"Failed to send event to Kafka: {e}")
    
    def run_generator(self):
        """Main generator loop."""
        logger.info(f"Starting streaming data generation at {DATA_RATE} events/sec")
        
        events_sent = 0
        start_time = time.time()
        
        try:
            while True:
                batch_start = time.time()
                events = []
                
                # Generate a batch of events
                for _ in range(DATA_RATE):
                    # Mix different event types
                    event_choice = random.random()
                    if event_choice < 0.6:
                        event = self.generate_user_event()
                    elif event_choice < 0.8:
                        event = self.generate_iot_sensor_event()
                    else:
                        event = self.generate_social_media_event()
                    
                    events.append(event)
                
                # Send batch to Kafka
                self.send_to_kafka_topics(events)
                events_sent += len(events)
                
                # Log progress
                if events_sent % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = events_sent / elapsed
                    logger.info(f"Sent {events_sent:,} events ({rate:.1f} events/sec)")
                
                # Sleep to maintain rate
                batch_duration = time.time() - batch_start
                sleep_time = max(0, 1.0 - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Streaming generator stopped")
        except Exception as e:
            logger.error(f"Generator error: {e}")
        finally:
            if self.producer:
                self.producer.close()
            
            elapsed = time.time() - start_time
            logger.info(f"Generated {events_sent:,} events in {elapsed:.1f} seconds")

if __name__ == "__main__":
    generator = StreamingDataGenerator()
    generator.run_generator()
