#!/usr/bin/env python3
"""
Simple Kafka Consumer for testing the data generator
"""

import json
import logging
from kafka import KafkaConsumer
import argparse
import signal
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaTestConsumer:
    """Simple Kafka consumer for testing data generation."""
    
    def __init__(self, topic: str, bootstrap_servers: list):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.running = False
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        
    def start_consuming(self):
        """Start consuming messages from the topic."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                group_id='test-consumer-group',
                auto_offset_reset='latest'
            )
            
            logger.info(f"Started consuming from topic: {self.topic}")
            self.running = True
            message_count = 0
            
            for message in self.consumer:
                if not self.running:
                    break
                    
                message_count += 1
                
                logger.info(f"Message {message_count}:")
                logger.info(f"  Topic: {message.topic}")
                logger.info(f"  Partition: {message.partition}")
                logger.info(f"  Offset: {message.offset}")
                logger.info(f"  Key: {message.key}")
                logger.info(f"  Value: {json.dumps(message.value, indent=2)}")
                logger.info("-" * 50)
                
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Consumer closed")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Kafka Test Consumer')
    parser.add_argument('--topic', '-t', required=True, help='Kafka topic to consume from')
    parser.add_argument('--bootstrap-servers', '-b', default='localhost:9092', 
                       help='Comma-separated list of bootstrap servers')
    
    args = parser.parse_args()
    
    bootstrap_servers = [server.strip() for server in args.bootstrap_servers.split(',')]
    
    try:
        consumer = KafkaTestConsumer(args.topic, bootstrap_servers)
        consumer.start_consuming()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()