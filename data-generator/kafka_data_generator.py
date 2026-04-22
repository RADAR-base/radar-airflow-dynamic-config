#!/usr/bin/env python3
"""
Kafka Data Generator
A flexible data generator that can produce various types of synthetic data and send them to Kafka topics.
"""

import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import yaml
import argparse
import signal
import sys

from kafka import KafkaProducer
from faker import Faker
import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaDataGenerator:
    """Main data generator class for producing synthetic data to Kafka topics."""

    def __init__(self, config_path: str):
        """Initialize the data generator with configuration."""
        self.config = self._load_config(config_path)
        self.fake = Faker()
        self.producer = None
        self.running = False

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self._setup_kafka_producer()
        self.fake_uuids = [self.fake.uuid4() for _ in range(10)]  # Pre-generate UUIDs for efficiency

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                logger.info(f"Configuration loaded from {config_path}")
                return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    def _setup_kafka_producer(self):
        """Setup Kafka producer with configuration."""
        kafka_config = self.config.get('kafka', {})

        producer_config = {
            'bootstrap_servers': kafka_config.get('bootstrap_servers', ['localhost:9092']),
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: str(k).encode('utf-8') if k else None,
            'retries': kafka_config.get('retries', 3),
            'batch_size': kafka_config.get('batch_size', 16384),
            'linger_ms': kafka_config.get('linger_ms', 10),
            'acks': kafka_config.get('acks', 'all')
        }

        try:
            self.producer = KafkaProducer(**producer_config)
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Kafka producer: {e}")
            raise

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def generate_user_data(self) -> Dict[str, Any]:
        """Generate fake user data."""
        return {
            'user_id': self.fake.uuid4(),
            'username': self.fake.user_name(),
            'email': self.fake.email(),
            'first_name': self.fake.first_name(),
            'last_name': self.fake.last_name(),
            'age': random.randint(18, 80),
            'country': self.fake.country(),
            'city': self.fake.city(),
            'phone': self.fake.phone_number(),
            'created_at': datetime.utcnow().isoformat(),
            'is_active': random.choice([True, False])
        }

    def generate_transaction_data(self) -> Dict[str, Any]:
        """Generate fake transaction data."""
        return {
            'transaction_id': self.fake.uuid4(),
            'user_id': self.fake.uuid4(),
            'amount': round(random.uniform(1.0, 1000.0), 2),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY', 'CAD']),
            'merchant': self.fake.company(),
            'category': random.choice(['food', 'transport', 'entertainment', 'shopping', 'utilities']),
            'status': random.choice(['completed', 'pending', 'failed']),
            'timestamp': datetime.utcnow().isoformat(),
            'description': self.fake.sentence(),
            'location': {
                'latitude': float(self.fake.latitude()),
                'longitude': float(self.fake.longitude())
            }
        }

    def generate_iot_sensor_data(self) -> Dict[str, Any]:
        """Generate IoT sensor data."""
        sensor_types = ['temperature', 'humidity', 'pressure', 'light', 'motion']
        sensor_type = random.choice(sensor_types)

        # Generate realistic values based on sensor type
        value_ranges = {
            'temperature': (-20, 50),
            'humidity': (0, 100),
            'pressure': (950, 1050),
            'light': (0, 1000),
            'motion': (0, 1)
        }

        min_val, max_val = value_ranges[sensor_type]

        return {
            'device_id': f"sensor_{random.randint(1, 100):03d}",
            'sensor_type': sensor_type,
            'value': round(random.uniform(min_val, max_val), 2),
            'unit': {
                'temperature': '°C',
                'humidity': '%',
                'pressure': 'hPa',
                'light': 'lux',
                'motion': 'boolean'
            }[sensor_type],
            'timestamp': datetime.utcnow().isoformat(),
            'location': {
                'building': f"Building_{random.randint(1, 10)}",
                'floor': random.randint(1, 20),
                'room': f"Room_{random.randint(1, 50):03d}"
            },
            'battery_level': random.randint(10, 100),
            'signal_strength': random.randint(-80, -20)
        }

    def generate_wearable_data(self) -> Dict[str, Any]:
        """Generate wearable per minute heart rate data"""
        return {
            'device_id': f"wearable_{random.randint(1, 100):03d}",
            'user_id': random.choice(self.fake_uuids),
            'heart_rate_bpm': random.randint(50, 120),
            'steps_count': random.randint(0, 500),
            'calories_burned': round(random.uniform(0.0, 50.0), 2),
            'timestamp': datetime.utcnow().isoformat()
        }

    def generate_clickstream_data(self) -> Dict[str, Any]:
        """Generate web clickstream data."""
        actions = ['page_view', 'click', 'scroll', 'form_submit', 'download', 'search']
        pages = ['/home', '/products', '/about', '/contact', '/login', '/checkout', '/profile']

        return {
            'session_id': self.fake.uuid4(),
            'user_id': self.fake.uuid4() if random.random() > 0.3 else None,  # Some anonymous users
            'action': random.choice(actions),
            'page': random.choice(pages),
            'timestamp': datetime.utcnow().isoformat(),
            'user_agent': self.fake.user_agent(),
            'ip_address': self.fake.ipv4(),
            'referrer': self.fake.url() if random.random() > 0.4 else None,
            'duration_ms': random.randint(100, 30000),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
            'device_type': random.choice(['desktop', 'mobile', 'tablet']),
            'coordinates': {
                'x': random.randint(0, 1920),
                'y': random.randint(0, 1080)
            } if random.random() > 0.7 else None
        }

    def generate_log_data(self) -> Dict[str, Any]:
        """Generate application log data."""
        log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
        services = ['user-service', 'payment-service', 'order-service', 'notification-service']

        level = random.choice(log_levels)

        # Generate realistic log messages based on level
        if level == 'ERROR' or level == 'FATAL':
            message = random.choice([
                "Database connection failed",
                "Payment processing error",
                "Authentication failed",
                "Service unavailable",
                "Timeout occurred"
            ])
        elif level == 'WARN':
            message = random.choice([
                "High memory usage detected",
                "Slow query detected",
                "Rate limit approaching",
                "Deprecated API usage"
            ])
        else:
            message = random.choice([
                "User login successful",
                "Order processed",
                "Email sent",
                "Cache updated",
                "Request processed"
            ])

        return {
            'timestamp': datetime.utcnow().isoformat(),
            'level': level,
            'service': random.choice(services),
            'message': message,
            'correlation_id': self.fake.uuid4(),
            'user_id': self.fake.uuid4() if random.random() > 0.5 else None,
            'duration_ms': random.randint(10, 5000),
            'status_code': random.choice([200, 201, 400, 401, 404, 500, 503]),
            'request_id': self.fake.uuid4()
        }

    def get_data_generator(self, data_type: str):
        """Get the appropriate data generator function."""
        generators = {
            'users': self.generate_user_data,
            'transactions': self.generate_transaction_data,
            'iot_sensors': self.generate_iot_sensor_data,
            'clickstream': self.generate_clickstream_data,
            'logs': self.generate_log_data,
            'wearable_data': self.generate_wearable_data
        }

        return generators.get(data_type)

    def send_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """Send a message to Kafka topic."""
        try:
            future = self.producer.send(topic, value=message, key=key)
            # Optional: wait for message to be sent (for debugging)
            # record_metadata = future.get(timeout=10)
            logger.debug(f"Message sent to topic '{topic}': {message}")
        except Exception as e:
            logger.error(f"Error sending message to topic '{topic}': {e}")

    def run_generator(self, data_type: str, topic: str, interval: float, count: Optional[int] = None, missing_data_percentage: int = 0):
        """Run a specific data generator."""
        generator_func = self.get_data_generator(data_type)
        if not generator_func:
            logger.error(f"Unknown data type: {data_type}")
            return

        logger.info(f"Starting {data_type} generator for topic '{topic}' with interval {interval}s")

        sent_count = 0
        self.running = True

        try:
            while self.running:
                if count and sent_count >= count:
                    logger.info(f"Reached target count of {count} messages")
                    break

                # Generate and send data
                data = generator_func()
                key = data.get('user_id') or data.get('device_id') or data.get('session_id')
                # Introduce missing data if specified
                if random.randint(1, 100) > missing_data_percentage:
                    self.send_message(topic, data, key)
                    sent_count += 1
                    if sent_count % 100 == 0:
                        logger.info(f"Sent {sent_count} messages to topic '{topic}'")
                else:
                    logger.debug(
                        f"Skipping message due to missing datapercentage at time {datetime.utcnow().isoformat()}")
                time.sleep(interval)

        except Exception as e:
            logger.error(f"Error in generator loop: {e}")
        finally:
            logger.info(f"Generator stopped. Total messages sent: {sent_count}")

    def run_all_generators(self):
        """Run all configured generators based on configuration."""
        import threading

        generators_config = self.config.get('generators', [])
        threads = []

        logger.info(f"Starting {len(generators_config)} generators")

        for gen_config in generators_config:
            if not gen_config.get('enabled', True):
                continue

            thread = threading.Thread(
                target=self.run_generator,
                args=(
                    gen_config['data_type'],
                    gen_config['topic'],
                    gen_config.get('interval', 1.0),
                    gen_config.get('count'),
                    gen_config.get('missing_data_percentage', 0)
                )
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)

        self.running = True

        try:
            # Wait for all threads
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            logger.info("Shutdown requested...")
        finally:
            self.running = False
            if self.producer:
                self.producer.close()
                logger.info("Kafka producer closed")

    def close(self):
        """Close the generator and cleanup resources."""
        self.running = False
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Kafka Data Generator')
    parser.add_argument('--config', '-c', default='config.yaml', help='Configuration file path')
    parser.add_argument('--data-type', '-t', help='Data type to generate (users, transactions, iot_sensors, clickstream, logs)')
    parser.add_argument('--topic', help='Kafka topic to send data to')
    parser.add_argument('--interval', '-i', type=float, default=1.0, help='Interval between messages in seconds')
    parser.add_argument('--count', '-n', type=int, help='Number of messages to generate (infinite if not specified)')

    args = parser.parse_args()

    try:
        generator = KafkaDataGenerator(args.config)

        if args.data_type and args.topic:
            # Run single generator
            generator.run_generator(args.data_type, args.topic, args.interval, args.count)
        else:
            # Run all configured generators
            generator.run_all_generators()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'generator' in locals():
            generator.close()

if __name__ == "__main__":
    main()