#!/bin/bash

# Kafka Data Generator Management Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    cat << EOF
Kafka Data Generator Management Script

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    start           Start all services (Kafka, Airflow, data generator)
    stop            Stop all services
    restart         Restart all services
    logs            Show data generator logs
    status          Show status of all services
    topics          List Kafka topics
    consume         Start a test consumer for a topic
    build           Build the data generator image
    clean           Stop and remove all containers
    
Airflow Commands:
    airflow-logs    Show Airflow logs
    airflow-reset   Reset Airflow database
    flower          Start Flower (Celery monitoring)

Options:
    --topic TOPIC   Topic name (for consume command)
    --follow        Follow logs (for logs command)
    --service NAME  Service name (for airflow-logs command)
    --help          Show this help message

Examples:
    $0 start                    # Start all services
    $0 logs --follow           # Follow data generator logs
    $0 airflow-logs --service webserver --follow  # Follow Airflow webserver logs
    $0 consume --topic user-events  # Consume from user-events topic
    $0 topics                  # List all Kafka topics
    $0 flower                  # Start Flower monitoring

EOF
}

wait_for_kafka() {
    log_info "Waiting for Kafka to be ready..."
    
    # Wait for Kafka to be available
    timeout=60
    while [ $timeout -gt 0 ]; do
        if docker-compose exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list > /dev/null 2>&1; then
            log_info "Kafka is ready!"
            return 0
        fi
        
        sleep 2
        timeout=$((timeout - 2))
    done
    
    log_error "Timeout waiting for Kafka to be ready"
    return 1
}

create_topics() {
    log_info "Creating Kafka topics..."
    
    topics=(
        "user-events"
        "transaction-events"
        "iot-sensor-data"
        "web-clickstream"
        "application-logs"
        "wearable-device-data"
    )
    
    for topic in "${topics[@]}"; do
        if docker-compose exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list | grep -q "^${topic}$"; then
            log_info "Topic '$topic' already exists"
        else
            docker-compose exec -T kafka-1 kafka-topics \
                --bootstrap-server kafka-1:9092 \
                --create \
                --topic "$topic" \
                --partitions 3 \
                --replication-factor 3 \
                --if-not-exists
            log_info "Created topic: $topic"
        fi
    done
}

start_services() {
    log_info "Starting Kafka cluster, Airflow, and data generator..."
    
    cd "$PROJECT_DIR"
    
    # Start infrastructure services first
    docker-compose up -d zookeeper-1 zookeeper-2 zookeeper-3
    sleep 10
    
    docker-compose up -d kafka-1 kafka-2 kafka-3
    sleep 15
    
    docker-compose up -d schema-registry-1 rest-proxy-1
    sleep 10
    
    # Start Airflow infrastructure
    docker-compose up -d airflow-postgres airflow-redis
    sleep 10
    
    # Initialize Airflow
    docker-compose up airflow-init
    
    # Start Airflow services
    docker-compose up -d airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
    sleep 10
    
    # Wait for Kafka and create topics
    wait_for_kafka
    create_topics
    
    # Start data generator and Kafka UI
    docker-compose up -d data-generator kafka-ui
    
    log_info "All services started successfully!"
    log_info "Access points:"
    log_info "  - Airflow UI: http://localhost:8081 (airflow/airflow)"
    log_info "  - Kafka UI: http://localhost:8090"
    log_info "  - Kafka REST Proxy: http://localhost:8082"
    log_info "  - Schema Registry: http://localhost:8081"
}

stop_services() {
    log_info "Stopping all services..."
    cd "$PROJECT_DIR"
    docker-compose down
    log_info "All services stopped"
}

restart_services() {
    stop_services
    sleep 5
    start_services
}

show_logs() {
    cd "$PROJECT_DIR"
    
    if [ "$1" = "--follow" ]; then
        docker-compose logs -f data-generator
    else
        docker-compose logs data-generator
    fi
}

show_status() {
    cd "$PROJECT_DIR"
    docker-compose ps
}

list_topics() {
    log_info "Listing Kafka topics..."
    cd "$PROJECT_DIR"
    docker-compose exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list
}

consume_topic() {
    local topic="$1"
    
    if [ -z "$topic" ]; then
        log_error "Topic name is required. Use --topic TOPIC_NAME"
        exit 1
    fi
    
    log_info "Starting consumer for topic: $topic"
    cd "$PROJECT_DIR"
    
    docker-compose exec kafka-1 kafka-console-consumer \
        --bootstrap-server kafka-1:9092 \
        --topic "$topic" \
        --from-beginning \
        --property print.key=true \
        --property print.value=true \
        --property key.separator=" : "
}

build_image() {
    log_info "Building data generator image..."
    cd "$PROJECT_DIR"
    docker-compose build data-generator
    log_info "Build completed"
}

clean_all() {
    log_info "Stopping and removing all containers..."
    cd "$PROJECT_DIR"
    docker-compose down -v --remove-orphans
    log_info "Cleanup completed"
}

show_airflow_logs() {
    local service="$1"
    local follow_flag="$2"
    
    cd "$PROJECT_DIR"
    
    if [ -z "$service" ]; then
        log_info "Available Airflow services: webserver, scheduler, worker, triggerer, postgres, redis"
        log_error "Please specify a service with --service SERVICE_NAME"
        exit 1
    fi
    
    service_name="airflow-${service}"
    
    log_info "Showing logs for Airflow service: $service"
    
    if [ "$follow_flag" = "--follow" ]; then
        docker-compose logs -f "$service_name"
    else
        docker-compose logs "$service_name"
    fi
}

reset_airflow() {
    log_info "Resetting Airflow database..."
    cd "$PROJECT_DIR"
    
    log_warn "This will destroy all Airflow data including task history, connections, and variables!"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose stop airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
        docker-compose down airflow-postgres
        docker volume rm "${PWD##*/}_airflow-postgres-db-volume" 2>/dev/null || true
        docker-compose up -d airflow-postgres
        sleep 10
        docker-compose up airflow-init
        docker-compose up -d airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
        log_info "Airflow database reset completed"
    else
        log_info "Reset cancelled"
    fi
}

start_flower() {
    log_info "Starting Flower (Celery monitoring)..."
    cd "$PROJECT_DIR"
    docker-compose --profile flower up -d flower
    log_info "Flower started at http://localhost:5555"
}

# Parse command line arguments
COMMAND=""
TOPIC=""
FOLLOW=""
SERVICE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        start|stop|restart|logs|status|topics|consume|build|clean|airflow-logs|airflow-reset|flower)
            COMMAND="$1"
            shift
            ;;
        --topic)
            TOPIC="$2"
            shift 2
            ;;
        --follow)
            FOLLOW="--follow"
            shift
            ;;
        --service)
            SERVICE="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Execute command
case "$COMMAND" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    logs)
        show_logs "$FOLLOW"
        ;;
    status)
        show_status
        ;;
    topics)
        list_topics
        ;;
    consume)
        consume_topic "$TOPIC"
        ;;
    build)
        build_image
        ;;
    clean)
        clean_all
        ;;
    airflow-logs)
        show_airflow_logs "$SERVICE" "$FOLLOW"
        ;;
    airflow-reset)
        reset_airflow
        ;;
    flower)
        start_flower
        ;;
    "")
        log_error "No command specified"
        show_help
        exit 1
        ;;
    *)
        log_error "Unknown command: $COMMAND"
        show_help
        exit 1
        ;;
esac