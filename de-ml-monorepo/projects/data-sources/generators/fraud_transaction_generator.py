"""
Fraud Detection Transaction Generator
Generates realistic credit card transactions with fraud patterns for thesis research
"""

import json
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import numpy as np
from faker import Faker
import pandas as pd
from kafka import KafkaProducer
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class FraudTransactionGenerator:
    """
    Generates realistic credit card transactions with various fraud patterns.
    Based on research patterns from fraud detection literature.
    """
    
    def __init__(self, fraud_rate: float = 0.02):
        self.fraud_rate = fraud_rate
        self.user_profiles = {}
        self.merchant_profiles = {}
        self.fraud_patterns = {
            'stolen_card': 0.3,      # Rapid high-value purchases
            'card_not_present': 0.25, # Online fraud
            'account_takeover': 0.2,  # Behavioral pattern change
            'friendly_fraud': 0.15,   # Legitimate customer disputes
            'synthetic_id': 0.1       # Fake identity
        }
        self._initialize_profiles()
        
    def _initialize_profiles(self, num_users: int = 10000, num_merchants: int = 5000):
        """Initialize user and merchant profiles for realistic data generation"""
        
        # User profiles with spending patterns
        for i in range(num_users):
            self.user_profiles[f"USER_{i:06d}"] = {
                'card_number': self._generate_card_number(),
                'avg_transaction': np.random.lognormal(3.5, 1.5),  # Log-normal spending
                'transaction_frequency': np.random.gamma(2, 2),     # Transactions per day
                'preferred_categories': np.random.choice(
                    ['grocery', 'gas', 'restaurant', 'online', 'entertainment', 'travel'],
                    size=3, replace=False
                ).tolist(),
                'home_location': (
                    fake.latitude(), 
                    fake.longitude()
                ),
                'risk_score': np.random.beta(2, 5),  # Most users are low risk
                'account_age_days': np.random.randint(30, 3650),
                'credit_limit': np.random.choice([1000, 2500, 5000, 10000, 25000], 
                                               p=[0.2, 0.3, 0.3, 0.15, 0.05])
            }
        
        # Merchant profiles
        categories = ['grocery', 'gas', 'restaurant', 'online', 'entertainment', 'travel', 'retail', 'other']
        for i in range(num_merchants):
            self.merchant_profiles[f"MERCH_{i:05d}"] = {
                'name': fake.company(),
                'category': np.random.choice(categories),
                'location': (fake.latitude(), fake.longitude()),
                'avg_ticket': np.random.lognormal(3.0, 1.2),
                'fraud_rate': np.random.beta(1, 50),  # Most merchants have low fraud
                'risk_score': np.random.uniform(0, 1)
            }
    
    def _generate_card_number(self) -> str:
        """Generate valid-looking card number (Luhn algorithm)"""
        prefix = random.choice(['4', '5', '37', '6'])  # Visa, MC, Amex, Discover
        length = 16 if prefix != '37' else 15
        
        number = prefix + ''.join([str(random.randint(0, 9)) for _ in range(length - len(prefix) - 1)])
        
        # Luhn algorithm for check digit
        digits = [int(d) for d in number]
        checksum = 0
        for i in range(len(digits) - 1, -1, -2):
            checksum += digits[i]
        for i in range(len(digits) - 2, -1, -2):
            doubled = digits[i] * 2
            checksum += doubled if doubled < 10 else doubled - 9
        
        check_digit = (10 - (checksum % 10)) % 10
        return number + str(check_digit)
    
    def _generate_normal_transaction(self, user_id: str, timestamp: datetime) -> Dict:
        """Generate normal (non-fraud) transaction based on user profile"""
        user = self.user_profiles[user_id]
        
        # Select merchant based on user preferences
        category = np.random.choice(user['preferred_categories'])
        eligible_merchants = [
            m_id for m_id, m in self.merchant_profiles.items() 
            if m['category'] == category
        ]
        merchant_id = np.random.choice(eligible_merchants) if eligible_merchants else np.random.choice(list(self.merchant_profiles.keys()))
        merchant = self.merchant_profiles[merchant_id]
        
        # Generate amount based on user and merchant patterns
        base_amount = (user['avg_transaction'] + merchant['avg_ticket']) / 2
        amount = round(abs(np.random.normal(base_amount, base_amount * 0.3)), 2)
        amount = min(amount, user['credit_limit'] * 0.5)  # Don't exceed 50% of credit limit normally
        
        return {
            'transaction_id': fake.uuid4(),
            'user_id': user_id,
            'card_number': user['card_number'][:4] + '*' * 8 + user['card_number'][-4:],  # Masked
            'merchant_id': merchant_id,
            'merchant_name': merchant['name'],
            'merchant_category': merchant['category'],
            'amount': amount,
            'currency': 'USD',
            'timestamp': timestamp.isoformat(),
            'location_lat': merchant['location'][0],
            'location_lon': merchant['location'][1],
            'channel': 'chip' if merchant['category'] != 'online' else 'online',
            'entry_mode': np.random.choice(['chip', 'swipe', 'tap', 'online'], p=[0.6, 0.2, 0.1, 0.1]),
            'is_international': np.random.random() < 0.05,
            'is_fraud': False,
            'fraud_type': None,
            'user_present': merchant['category'] != 'online',
            'days_since_last_transaction': np.random.randint(0, 7),
            'hour_of_day': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'is_weekend': timestamp.weekday() >= 5
        }
    
    def _generate_fraud_transaction(self, user_id: str, timestamp: datetime, fraud_type: str) -> Dict:
        """Generate fraudulent transaction based on fraud pattern"""
        transaction = self._generate_normal_transaction(user_id, timestamp)
        user = self.user_profiles[user_id]
        
        if fraud_type == 'stolen_card':
            # Rapid high-value purchases in different locations
            transaction['amount'] = round(np.random.uniform(
                user['credit_limit'] * 0.3, 
                user['credit_limit'] * 0.8
            ), 2)
            transaction['location_lat'] += np.random.uniform(-5, 5)  # Different location
            transaction['location_lon'] += np.random.uniform(-5, 5)
            transaction['days_since_last_transaction'] = 0  # Rapid succession
            
        elif fraud_type == 'card_not_present':
            # Online fraud, often international
            online_merchants = [
                m_id for m_id, m in self.merchant_profiles.items() 
                if m['category'] == 'online'
            ]
            if online_merchants:
                transaction['merchant_id'] = np.random.choice(online_merchants)
            transaction['channel'] = 'online'
            transaction['entry_mode'] = 'online'
            transaction['user_present'] = False
            transaction['is_international'] = np.random.random() < 0.4
            transaction['amount'] *= np.random.uniform(1.5, 3)
            
        elif fraud_type == 'account_takeover':
            # Sudden behavior change
            transaction['amount'] = user['avg_transaction'] * np.random.uniform(5, 10)
            new_category = np.random.choice(
                [c for c in ['luxury', 'electronics', 'jewelry'] 
                 if c not in user['preferred_categories']]
            )
            transaction['merchant_category'] = new_category
            transaction['hour_of_day'] = np.random.choice([2, 3, 4, 5])  # Unusual hours
            
        elif fraud_type == 'friendly_fraud':
            # Legitimate-looking transaction that will be disputed
            transaction['amount'] *= np.random.uniform(1.2, 2)
            # Keep most patterns normal
            
        elif fraud_type == 'synthetic_id':
            # New account with aggressive spending
            if user['account_age_days'] < 90:
                transaction['amount'] = user['credit_limit'] * np.random.uniform(0.5, 0.9)
        
        transaction['is_fraud'] = True
        transaction['fraud_type'] = fraud_type
        
        # Add risk indicators
        transaction['velocity_1h'] = np.random.randint(3, 10) if fraud_type == 'stolen_card' else 1
        transaction['velocity_24h'] = np.random.randint(10, 30) if fraud_type in ['stolen_card', 'account_takeover'] else np.random.randint(1, 5)
        
        return transaction
    
    def generate_transaction_stream(self, duration_hours: int = 24, 
                                  transactions_per_second: float = 10) -> List[Dict]:
        """Generate a stream of transactions over specified duration"""
        transactions = []
        current_time = datetime.now(timezone.utc)
        end_time = current_time + timedelta(hours=duration_hours)
        
        while current_time < end_time:
            # Poisson process for transaction arrivals
            interval = np.random.exponential(1 / transactions_per_second)
            current_time += timedelta(seconds=interval)
            
            # Select random user
            user_id = np.random.choice(list(self.user_profiles.keys()))
            
            # Determine if fraud based on rate
            is_fraud = np.random.random() < self.fraud_rate
            
            if is_fraud:
                fraud_type = np.random.choice(
                    list(self.fraud_patterns.keys()),
                    p=list(self.fraud_patterns.values())
                )
                transaction = self._generate_fraud_transaction(user_id, current_time, fraud_type)
            else:
                transaction = self._generate_normal_transaction(user_id, current_time)
            
            # Add derived features useful for ML
            transaction['amount_zscore'] = round((transaction['amount'] - self.user_profiles[user_id]['avg_transaction']) / 
                                               (self.user_profiles[user_id]['avg_transaction'] * 0.3), 2)
            
            transactions.append(transaction)
        
        return transactions
    
    def stream_to_kafka(self, topic: str = 'fraud-transactions', 
                       bootstrap_servers: str = 'localhost:9092',
                       rate: float = 10.0):
        """Stream transactions to Kafka in real-time"""
        
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        logger.info(f"Starting fraud transaction stream to {topic} at {rate} tps")
        
        try:
            while True:
                # Generate single transaction
                user_id = np.random.choice(list(self.user_profiles.keys()))
                timestamp = datetime.now(timezone.utc)
                
                is_fraud = np.random.random() < self.fraud_rate
                
                if is_fraud:
                    fraud_type = np.random.choice(
                        list(self.fraud_patterns.keys()),
                        p=list(self.fraud_patterns.values())
                    )
                    transaction = self._generate_fraud_transaction(user_id, timestamp, fraud_type)
                else:
                    transaction = self._generate_normal_transaction(user_id, timestamp)
                
                # Send to Kafka
                producer.send(
                    topic,
                    key=user_id,
                    value=transaction
                )
                
                # Log every 100th transaction
                if np.random.random() < 0.01:
                    logger.info(f"Sent transaction: fraud={transaction['is_fraud']}, "
                              f"amount=${transaction['amount']}, "
                              f"type={transaction.get('fraud_type', 'normal')}")
                
                # Control rate
                time.sleep(1.0 / rate)
                
        except KeyboardInterrupt:
            logger.info("Shutting down fraud transaction stream")
        finally:
            producer.flush()
            producer.close()
    
    def generate_daily_batch(self, date: datetime, 
                           daily_volume: int = 1000000) -> pd.DataFrame:
        """Generate daily batch of transactions for historical analysis"""
        
        logger.info(f"Generating {daily_volume:,} transactions for {date.date()}")
        
        transactions = []
        start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Simulate daily pattern with peak hours
        hourly_distribution = np.array([
            0.02, 0.01, 0.01, 0.01, 0.02, 0.03,  # 00-05: Low activity
            0.04, 0.05, 0.06, 0.08, 0.09, 0.08,  # 06-11: Morning rise  
            0.10, 0.09, 0.08, 0.07, 0.06, 0.07,  # 12-17: Afternoon
            0.08, 0.07, 0.05, 0.04, 0.03, 0.02   # 18-23: Evening decline
        ])
        hourly_distribution = hourly_distribution / hourly_distribution.sum()
        
        hourly_volumes = np.random.multinomial(daily_volume, hourly_distribution)
        
        for hour, volume in enumerate(hourly_volumes):
            hour_start = start_time + timedelta(hours=hour)
            
            for _ in range(volume):
                # Random minute within the hour
                minute_offset = np.random.randint(0, 60)
                second_offset = np.random.uniform(0, 60)
                
                timestamp = hour_start + timedelta(minutes=minute_offset, seconds=second_offset)
                
                user_id = np.random.choice(list(self.user_profiles.keys()))
                
                is_fraud = np.random.random() < self.fraud_rate
                
                if is_fraud:
                    fraud_type = np.random.choice(
                        list(self.fraud_patterns.keys()),
                        p=list(self.fraud_patterns.values())
                    )
                    transaction = self._generate_fraud_transaction(user_id, timestamp, fraud_type)
                else:
                    transaction = self._generate_normal_transaction(user_id, timestamp)
                
                transactions.append(transaction)
        
        df = pd.DataFrame(transactions)
        
        # Add additional features for ML
        df['amount_log'] = np.log1p(df['amount'])
        df['is_high_amount'] = df['amount'] > df['amount'].quantile(0.95)
        
        logger.info(f"Generated {len(df)} transactions with {df['is_fraud'].sum()} frauds "
                   f"({df['is_fraud'].mean()*100:.2f}% fraud rate)")
        
        return df


if __name__ == "__main__":
    # Configuration from environment
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'fraud-transactions')
    FRAUD_RATE = float(os.getenv('FRAUD_RATE', '0.02'))
    TRANSACTION_RATE = float(os.getenv('TRANSACTION_RATE', '10'))
    MODE = os.getenv('MODE', 'stream')  # 'stream' or 'batch'
    
    generator = FraudTransactionGenerator(fraud_rate=FRAUD_RATE)
    
    if MODE == 'stream':
        # Real-time streaming mode
        generator.stream_to_kafka(
            topic=KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            rate=TRANSACTION_RATE
        )
    else:
        # Batch generation mode
        today = datetime.now(timezone.utc)
        df = generator.generate_daily_batch(today, daily_volume=100000)
        
        # Save to parquet
        output_path = f"fraud_transactions_{today.strftime('%Y%m%d')}.parquet"
        df.to_parquet(output_path, engine='pyarrow', compression='snappy')
        logger.info(f"Saved batch to {output_path}")
