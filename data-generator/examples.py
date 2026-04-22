#!/usr/bin/env python3
"""
Example script showing how to use the Kafka Data Generator programmatically
"""

import time
import threading
from kafka_data_generator import KafkaDataGenerator


def run_custom_generator():
    """Example of running a custom data generator configuration."""
    
    # Custom configuration for demo
    config = {
        'kafka': {
            'bootstrap_servers': ['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 10,
            'acks': 'all'
        },
        'generators': [
            {
                'data_type': 'users',
                'topic': 'demo-users',
                'interval': 1.0,
                'enabled': True
            },
            {
                'data_type': 'transactions',
                'topic': 'demo-transactions', 
                'interval': 0.5,
                'enabled': True
            }
        ]
    }
    
    # Create temporary config file
    import yaml
    with open('demo_config.yaml', 'w') as f:
        yaml.dump(config, f)
    
    try:
        # Initialize generator
        generator = KafkaDataGenerator('demo_config.yaml')
        
        print("Starting demo data generation...")
        print("Generating users every 1 second to 'demo-users' topic")
        print("Generating transactions every 0.5 seconds to 'demo-transactions' topic")
        print("Press Ctrl+C to stop")
        
        # Start generators in separate threads
        user_thread = threading.Thread(
            target=generator.run_generator,
            args=('users', 'demo-users', 1.0, 50)  # Generate 50 users
        )
        
        transaction_thread = threading.Thread(
            target=generator.run_generator,
            args=('transactions', 'demo-transactions', 0.5, 100)  # Generate 100 transactions
        )
        
        user_thread.daemon = True
        transaction_thread.daemon = True
        
        user_thread.start()
        transaction_thread.start()
        
        # Wait for threads to complete
        user_thread.join()
        transaction_thread.join()
        
        print("Demo completed!")
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Error running demo: {e}")
    finally:
        if 'generator' in locals():
            generator.close()
        
        # Cleanup
        import os
        if os.path.exists('demo_config.yaml'):
            os.remove('demo_config.yaml')


def single_message_example():
    """Example of generating and sending single messages."""
    
    config = {
        'kafka': {
            'bootstrap_servers': ['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092']
        }
    }
    
    # Create temporary config
    import yaml
    with open('single_config.yaml', 'w') as f:
        yaml.dump(config, f)
    
    try:
        generator = KafkaDataGenerator('single_config.yaml')
        
        print("Generating single messages...")
        
        # Generate individual messages
        user_data = generator.generate_user_data()
        generator.send_message('test-users', user_data, user_data['user_id'])
        print(f"Sent user: {user_data['username']}")
        
        transaction_data = generator.generate_transaction_data()
        generator.send_message('test-transactions', transaction_data, transaction_data['transaction_id'])
        print(f"Sent transaction: ${transaction_data['amount']}")
        
        iot_data = generator.generate_iot_sensor_data()
        generator.send_message('test-iot', iot_data, iot_data['device_id'])
        print(f"Sent IoT reading: {iot_data['sensor_type']} = {iot_data['value']}")
        
        print("Single message example completed!")
        
    except Exception as e:
        print(f"Error in single message example: {e}")
    finally:
        if 'generator' in locals():
            generator.close()
        
        # Cleanup
        import os
        if os.path.exists('single_config.yaml'):
            os.remove('single_config.yaml')


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Data Generator Examples')
    parser.add_argument('--mode', choices=['demo', 'single'], default='demo',
                       help='Example mode to run')
    
    args = parser.parse_args()
    
    if args.mode == 'demo':
        run_custom_generator()
    elif args.mode == 'single':
        single_message_example()