#!/usr/bin/env python3
"""
Financial Market Data Generator
Generates realistic financial market data for time-series analysis and algorithmic trading simulations.
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

class MarketDataGenerator:
    def __init__(self):
        self.producer = None
        self.connect_to_kafka()
        
        # Financial instruments
        self.stocks = [
            {'symbol': 'AAPL', 'company': 'Apple Inc.', 'sector': 'Technology', 'base_price': 180.0},
            {'symbol': 'GOOGL', 'company': 'Alphabet Inc.', 'sector': 'Technology', 'base_price': 140.0},
            {'symbol': 'MSFT', 'company': 'Microsoft Corp.', 'sector': 'Technology', 'base_price': 380.0},
            {'symbol': 'AMZN', 'company': 'Amazon.com Inc.', 'sector': 'Consumer Discretionary', 'base_price': 150.0},
            {'symbol': 'TSLA', 'company': 'Tesla Inc.', 'sector': 'Automotive', 'base_price': 220.0},
            {'symbol': 'META', 'company': 'Meta Platforms Inc.', 'sector': 'Technology', 'base_price': 320.0},
            {'symbol': 'NVDA', 'company': 'NVIDIA Corp.', 'sector': 'Technology', 'base_price': 450.0},
            {'symbol': 'NFLX', 'company': 'Netflix Inc.', 'sector': 'Media', 'base_price': 420.0},
            {'symbol': 'JPM', 'company': 'JPMorgan Chase', 'sector': 'Financial', 'base_price': 150.0},
            {'symbol': 'JNJ', 'company': 'Johnson & Johnson', 'sector': 'Healthcare', 'base_price': 160.0}
        ]
        
        self.crypto = [
            {'symbol': 'BTC-USD', 'name': 'Bitcoin', 'base_price': 65000.0},
            {'symbol': 'ETH-USD', 'name': 'Ethereum', 'base_price': 3500.0},
            {'symbol': 'BNB-USD', 'name': 'Binance Coin', 'base_price': 320.0},
            {'symbol': 'ADA-USD', 'name': 'Cardano', 'base_price': 0.5},
            {'symbol': 'SOL-USD', 'name': 'Solana', 'base_price': 140.0}
        ]
        
        self.forex = [
            {'symbol': 'EUR/USD', 'name': 'Euro to US Dollar', 'base_price': 1.08},
            {'symbol': 'GBP/USD', 'name': 'British Pound to US Dollar', 'base_price': 1.25},
            {'symbol': 'USD/JPY', 'name': 'US Dollar to Japanese Yen', 'base_price': 150.0},
            {'symbol': 'AUD/USD', 'name': 'Australian Dollar to US Dollar', 'base_price': 0.65},
            {'symbol': 'USD/CAD', 'name': 'US Dollar to Canadian Dollar', 'base_price': 1.35}
        ]
        
        # Price tracking for realistic movements
        self.current_prices = {}
        self._initialize_prices()
        
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
    
    def _initialize_prices(self):
        """Initialize current prices for all instruments."""
        all_instruments = self.stocks + self.crypto + self.forex
        
        for instrument in all_instruments:
            symbol = instrument['symbol']
            base_price = instrument['base_price']
            
            # Add some initial randomness (±5%)
            variation = random.uniform(-0.05, 0.05)
            current_price = base_price * (1 + variation)
            
            self.current_prices[symbol] = {
                'price': current_price,
                'high': current_price,
                'low': current_price,
                'volume': 0,
                'last_update': datetime.utcnow()
            }
    
    def generate_stock_tick(self, stock):
        """Generate realistic stock price tick."""
        symbol = stock['symbol']
        current_data = self.current_prices[symbol]
        current_price = current_data['price']
        
        # Market volatility simulation
        now = datetime.utcnow()
        hour = now.hour
        
        # Higher volatility during market hours (9:30 AM - 4:00 PM EST)
        if 9 <= hour <= 16:
            volatility = 0.002  # 0.2% standard deviation
            volume_multiplier = 1.0
        else:
            volatility = 0.0005  # 0.05% standard deviation (after hours)
            volume_multiplier = 0.1
        
        # Generate price movement
        price_change = np.random.normal(0, volatility * current_price)
        new_price = max(0.01, current_price + price_change)
        
        # Generate volume (realistic trading volume)
        base_volume = random.randint(100, 10000) * volume_multiplier
        volume = max(1, int(base_volume))
        
        # Update tracking data
        self.current_prices[symbol].update({
            'price': new_price,
            'high': max(current_data['high'], new_price),
            'low': min(current_data['low'], new_price),
            'volume': current_data['volume'] + volume,
            'last_update': now
        })
        
        # Calculate additional metrics
        price_change_pct = ((new_price - current_price) / current_price) * 100
        
        return {
            'event_id': fake.uuid4(),
            'symbol': symbol,
            'company': stock['company'],
            'sector': stock['sector'],
            'timestamp': now,
            'price': round(new_price, 2),
            'volume': volume,
            'high': round(current_data['high'], 2),
            'low': round(current_data['low'], 2),
            'change': round(new_price - current_price, 2),
            'change_percent': round(price_change_pct, 4),
            'bid': round(new_price - random.uniform(0.01, 0.05), 2),
            'ask': round(new_price + random.uniform(0.01, 0.05), 2),
            'market_cap': random.randint(100000000, 3000000000000),
            'asset_type': 'stock'
        }
    
    def generate_crypto_tick(self, crypto):
        """Generate realistic cryptocurrency price tick."""
        symbol = crypto['symbol']
        current_data = self.current_prices[symbol]
        current_price = current_data['price']
        
        # Crypto has higher volatility (24/7 trading)
        volatility = 0.01  # 1% standard deviation
        
        # Generate price movement
        price_change = np.random.normal(0, volatility * current_price)
        new_price = max(0.0001, current_price + price_change)
        
        # Crypto volume patterns
        volume = random.uniform(0.1, 100.0)  # BTC equivalent
        
        # Update tracking data
        self.current_prices[symbol].update({
            'price': new_price,
            'high': max(current_data['high'], new_price),
            'low': min(current_data['low'], new_price),
            'volume': current_data['volume'] + volume,
            'last_update': datetime.utcnow()
        })
        
        price_change_pct = ((new_price - current_price) / current_price) * 100
        
        return {
            'event_id': fake.uuid4(),
            'symbol': symbol,
            'name': crypto['name'],
            'timestamp': datetime.utcnow(),
            'price': round(new_price, 2 if new_price > 1 else 8),
            'volume': round(volume, 4),
            'high': round(current_data['high'], 2 if current_data['high'] > 1 else 8),
            'low': round(current_data['low'], 2 if current_data['low'] > 1 else 8),
            'change': round(new_price - current_price, 8),
            'change_percent': round(price_change_pct, 4),
            'market_cap': random.randint(1000000000, 1200000000000),
            'circulating_supply': random.randint(1000000, 21000000),
            'asset_type': 'cryptocurrency'
        }
    
    def generate_forex_tick(self, forex_pair):
        """Generate realistic forex price tick."""
        symbol = forex_pair['symbol']
        current_data = self.current_prices[symbol]
        current_price = current_data['price']
        
        # Forex has lower volatility
        volatility = 0.0005  # 0.05% standard deviation
        
        # Generate price movement
        price_change = np.random.normal(0, volatility * current_price)
        new_price = max(0.0001, current_price + price_change)
        
        # Forex volume (in lots)
        volume = random.uniform(1.0, 1000.0)
        
        # Update tracking data
        self.current_prices[symbol].update({
            'price': new_price,
            'high': max(current_data['high'], new_price),
            'low': min(current_data['low'], new_price),
            'volume': current_data['volume'] + volume,
            'last_update': datetime.utcnow()
        })
        
        price_change_pct = ((new_price - current_price) / current_price) * 100
        
        return {
            'event_id': fake.uuid4(),
            'symbol': symbol,
            'name': forex_pair['name'],
            'timestamp': datetime.utcnow(),
            'price': round(new_price, 5),
            'volume': round(volume, 2),
            'high': round(current_data['high'], 5),
            'low': round(current_data['low'], 5),
            'change': round(new_price - current_price, 6),
            'change_percent': round(price_change_pct, 4),
            'spread': round(random.uniform(0.0001, 0.001), 5),
            'asset_type': 'forex'
        }
    
    def generate_market_event(self):
        """Generate a random market event."""
        # Randomly select asset type
        asset_type = np.random.choice(['stock', 'crypto', 'forex'], p=[0.6, 0.3, 0.1])
        
        if asset_type == 'stock':
            instrument = random.choice(self.stocks)
            return self.generate_stock_tick(instrument)
        elif asset_type == 'crypto':
            instrument = random.choice(self.crypto)
            return self.generate_crypto_tick(instrument)
        else:  # forex
            instrument = random.choice(self.forex)
            return self.generate_forex_tick(instrument)
    
    def generate_market_summary(self):
        """Generate market summary/index data."""
        indices = [
            {'symbol': 'SPY', 'name': 'S&P 500 ETF', 'base_value': 450.0},
            {'symbol': 'QQQ', 'name': 'NASDAQ 100 ETF', 'base_value': 380.0},
            {'symbol': 'DIA', 'name': 'Dow Jones ETF', 'base_value': 340.0}
        ]
        
        summary_data = []
        
        for index in indices:
            # Calculate index movement
            price_change = np.random.normal(0, 0.01 * index['base_value'])
            current_value = max(1.0, index['base_value'] + price_change)
            
            change_pct = (price_change / index['base_value']) * 100
            
            summary = {
                'event_id': fake.uuid4(),
                'symbol': index['symbol'],
                'name': index['name'],
                'timestamp': datetime.utcnow(),
                'value': round(current_value, 2),
                'change': round(price_change, 2),
                'change_percent': round(change_pct, 3),
                'volume': random.randint(1000000, 100000000),
                'market_sentiment': random.choice(['bullish', 'bearish', 'neutral']),
                'volatility_index': round(random.uniform(10.0, 40.0), 2),
                'asset_type': 'index'
            }
            
            summary_data.append(summary)
        
        return summary_data
    
    def run_market_data_generation(self):
        """Run continuous market data generation."""
        logger.info(f"Starting market data generation")
        logger.info(f"Instruments: {len(self.stocks)} stocks, {len(self.crypto)} crypto, {len(self.forex)} forex")
        
        events_sent = 0
        start_time = time.time()
        
        try:
            while True:
                batch_start = time.time()
                
                # Generate market events
                for _ in range(DATA_RATE):
                    # 90% individual ticks, 10% market summaries
                    if random.random() < 0.9:
                        event = self.generate_market_event()
                        topic = f"market_{event['asset_type']}"
                    else:
                        # Generate market summary
                        summaries = self.generate_market_summary()
                        for summary in summaries:
                            try:
                                self.producer.send(
                                    'market_indices',
                                    value=summary,
                                    key=summary['symbol']
                                )
                                events_sent += 1
                            except Exception as e:
                                logger.error(f"Failed to send market summary: {e}")
                        continue
                    
                    # Send to appropriate Kafka topic
                    try:
                        self.producer.send(
                            topic,
                            value=event,
                            key=event['symbol']
                        )
                        events_sent += 1
                    except Exception as e:
                        logger.error(f"Failed to send market event: {e}")
                
                # Flush producer periodically
                if events_sent % 1000 == 0:
                    self.producer.flush()
                
                # Log progress
                if events_sent % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = events_sent / elapsed
                    logger.info(f"Sent {events_sent:,} market events ({rate:.1f} events/sec)")
                    
                    # Show sample current prices
                    sample_symbols = ['AAPL', 'BTC-USD', 'EUR/USD']
                    for symbol in sample_symbols:
                        if symbol in self.current_prices:
                            price_data = self.current_prices[symbol]
                            logger.info(f"  {symbol}: ${price_data['price']:.2f}")
                
                # Reset daily highs/lows at midnight
                now = datetime.utcnow()
                if now.hour == 0 and now.minute == 0:
                    self._reset_daily_stats()
                
                # Maintain rate
                batch_duration = time.time() - batch_start
                sleep_time = max(0, 1.0 - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Market data generator stopped")
        except Exception as e:
            logger.error(f"Market data generator error: {e}")
        finally:
            if self.producer:
                self.producer.close()
            
            elapsed = time.time() - start_time
            logger.info(f"Generated {events_sent:,} market events in {elapsed:.1f} seconds")
    
    def _reset_daily_stats(self):
        """Reset daily high/low statistics."""
        for symbol in self.current_prices:
            current_price = self.current_prices[symbol]['price']
            self.current_prices[symbol].update({
                'high': current_price,
                'low': current_price,
                'volume': 0
            })
        logger.info("Daily market statistics reset")

if __name__ == "__main__":
    generator = MarketDataGenerator()
    generator.run_market_data_generation()
