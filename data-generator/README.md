# Kafka Data Generator

A flexible and configurable data generator for Apache Kafka that produces various types of synthetic data for testing, development, and demonstration purposes.

## Features

- **Multiple Data Types**: Generates realistic synthetic data for:
  - User profiles and registration data
  - Financial transactions
  - IoT sensor readings
  - Web clickstream events
  - Application logs

- **Configurable**: Easy-to-modify YAML configuration for topics, intervals, and data types
- **Scalable**: Multi-threaded generation with different intervals per data type
- **Docker Ready**: Containerized and integrated with Docker Compose
- **Kafka Integration**: Built-in Kafka producer with proper error handling
- **Graceful Shutdown**: Signal handling for clean shutdowns

## Quick Start

### 1. Start the Environment

```bash
# Start all services (Kafka cluster + data generator)
./data-generator/manage.sh start

# Or start manually with docker-compose
docker-compose up -d
```

### 2. Check Status

```bash
# View service status
./data-generator/manage.sh status

# View data generator logs
./data-generator/manage.sh logs --follow
```

### 3. List Topics

```bash
# List all Kafka topics
./data-generator/manage.sh topics
```

### 4. Consume Data

```bash
# Consume from a specific topic
./data-generator/manage.sh consume --topic user-events

# Or consume using Kafka console consumer
docker-compose exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic user-events \
  --from-beginning
```

## Configuration

The data generator is configured via `config.yaml`:

```yaml
kafka:
  bootstrap_servers:
    - "kafka-1:9092"
    - "kafka-2:9092" 
    - "kafka-3:9092"
  retries: 3
  batch_size: 16384
  linger_ms: 10
  acks: "all"

generators:
  - data_type: "users"
    topic: "user-events"
    interval: 2.0
    enabled: true
    
  - data_type: "transactions"
    topic: "transaction-events"
    interval: 1.5
    enabled: true
```

### Configuration Options

- **data_type**: Type of data to generate (`users`, `transactions`, `iot_sensors`, `clickstream`, `logs`)
- **topic**: Kafka topic to send data to
- **interval**: Time between messages in seconds
- **enabled**: Whether this generator is active
- **count**: Optional limit on number of messages (infinite if omitted)

## Data Types

### 1. User Data (`users`)
Generates user registration and profile data:
```json
{
  "user_id": "uuid",
  "username": "string",
  "email": "email@domain.com",
  "first_name": "string",
  "last_name": "string",
  "age": 25,
  "country": "string",
  "city": "string",
  "created_at": "2024-01-01T12:00:00",
  "is_active": true
}
```

### 2. Transaction Data (`transactions`)
Generates financial transaction events:
```json
{
  "transaction_id": "uuid",
  "user_id": "uuid",
  "amount": 99.99,
  "currency": "USD",
  "merchant": "Company Name",
  "category": "shopping",
  "status": "completed",
  "timestamp": "2024-01-01T12:00:00",
  "location": {
    "latitude": 40.7128,
    "longitude": -74.0060
  }
}
```

### 3. IoT Sensor Data (`iot_sensors`)
Generates IoT device sensor readings:
```json
{
  "device_id": "sensor_001",
  "sensor_type": "temperature",
  "value": 23.5,
  "unit": "°C",
  "timestamp": "2024-01-01T12:00:00",
  "location": {
    "building": "Building_1",
    "floor": 2,
    "room": "Room_001"
  },
  "battery_level": 85,
  "signal_strength": -45
}
```

### 4. Clickstream Data (`clickstream`)
Generates web analytics events:
```json
{
  "session_id": "uuid",
  "user_id": "uuid",
  "action": "page_view",
  "page": "/products",
  "timestamp": "2024-01-01T12:00:00",
  "user_agent": "Mozilla/5.0...",
  "ip_address": "192.168.1.1",
  "device_type": "desktop"
}
```

### 5. Application Logs (`logs`)
Generates application log events:
```json
{
  "timestamp": "2024-01-01T12:00:00",
  "level": "INFO",
  "service": "user-service",
  "message": "User login successful",
  "correlation_id": "uuid",
  "duration_ms": 150,
  "status_code": 200
}
```

## Management Script

The `manage.sh` script provides convenient commands:

```bash
# Service Management
./data-generator/manage.sh start     # Start all services
./data-generator/manage.sh stop      # Stop all services
./data-generator/manage.sh restart   # Restart services
./data-generator/manage.sh status    # Show service status

# Monitoring
./data-generator/manage.sh logs --follow  # Follow logs
./data-generator/manage.sh topics         # List Kafka topics

# Testing
./data-generator/manage.sh consume --topic user-events  # Consume topic

# Maintenance
./data-generator/manage.sh build     # Rebuild images
./data-generator/manage.sh clean     # Remove all containers
```

## Manual Usage

You can also run the data generator manually:

```bash
# Run all configured generators
python kafka_data_generator.py --config config.yaml

# Run a specific generator
python kafka_data_generator.py \
  --data-type users \
  --topic test-topic \
  --interval 1.0 \
  --count 100
```

## Testing

Test the data generation with the included consumer:

```python
# Install dependencies
pip install -r requirements.txt

# Run test consumer
python test_consumer.py --topic user-events --bootstrap-servers kafka-1:9092
```

## Kafka Cluster

The included Kafka cluster consists of:
- 3 Zookeeper nodes
- 3 Kafka brokers
- Schema Registry
- REST Proxy
- Data Generator

### Ports
- Kafka REST Proxy: `8082`
- Schema Registry: `8081`

## Customization

### Adding New Data Types

1. Add a new generator method in `kafka_data_generator.py`:
```python
def generate_custom_data(self) -> Dict[str, Any]:
    return {
        "id": self.fake.uuid4(),
        "custom_field": "custom_value",
        "timestamp": datetime.utcnow().isoformat()
    }
```

2. Update the `get_data_generator` method to include your new type

3. Add configuration in `config.yaml`

### Modifying Data Schemas

Edit the generator methods in `kafka_data_generator.py` to modify the data structure and add new fields.

## Troubleshooting

### Common Issues

1. **Kafka not ready**: Wait a few minutes for Kafka cluster to fully start
2. **Topics not created**: Use `./data-generator/manage.sh topics` to verify
3. **Connection errors**: Check that all Kafka brokers are running

### Debug Mode

Enable debug logging by modifying `config.yaml`:
```yaml
logging:
  level: "DEBUG"
```

### Manual Topic Creation

```bash
docker-compose exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:9092 \
  --create \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 3
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Add tests if applicable
4. Update documentation
5. Submit a pull request

## License

This project is licensed under the MIT License.