#!/bin/bash
set -e

# Ultimate Big Data Platform Launcher
# Provides easy commands to launch different configurations of the platform

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_banner() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║                    🚀 Ultimate Big Data Platform 🚀                  ║"
    echo "║                                                                      ║"
    echo "║  Five enterprise-grade data engineering projects in one platform     ║"
    echo "║  Apache Kafka • Spark • Flink • Hadoop • Airflow • dbt • ML         ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

show_help() {
    echo -e "${CYAN}Available Commands:${NC}"
    echo ""
    echo -e "${GREEN}🚀 Basic Stack (Projects 1-3):${NC}"
    echo "  ./launch-platform.sh start              # Start basic platform"
    echo "  ./launch-platform.sh stop               # Stop all services"
    echo ""
    echo -e "${YELLOW}⚡ Advanced Options:${NC}"
    echo "  ./launch-platform.sh flink              # + Apache Flink streaming"
    echo "  ./launch-platform.sh hadoop             # + Hadoop batch processing"
    echo "  ./launch-platform.sh bigdata            # + Big Data generators"
    echo "  ./launch-platform.sh full               # Full platform (all components)"
    echo ""
    echo -e "${PURPLE}🔧 Management:${NC}"
    echo "  ./launch-platform.sh status             # Show service status"
    echo "  ./launch-platform.sh logs [service]     # Show service logs"
    echo "  ./launch-platform.sh clean              # Clean up all data"
    echo "  ./launch-platform.sh rebuild            # Rebuild and restart"
    echo ""
    echo -e "${BLUE}📊 Monitoring:${NC}"
    echo "  ./launch-platform.sh ui                 # Open all web interfaces"
    echo "  ./launch-platform.sh test               # Run platform tests"
}

check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker is not installed${NC}"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo -e "${RED}❌ Docker Compose is not installed${NC}"
        exit 1
    fi
    
    # Check available disk space (at least 10GB)
    available_space=$(df . | tail -1 | awk '{print $4}')
    if [ $available_space -lt 10485760 ]; then  # 10GB in KB
        echo -e "${RED}⚠️  Warning: Less than 10GB disk space available${NC}"
        echo -e "${YELLOW}   This platform may require significant disk space${NC}"
    fi
    
    # Check available memory (at least 8GB)
    if [ -f /proc/meminfo ]; then
        memory_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')
        if [ $memory_kb -lt 8388608 ]; then  # 8GB in KB
            echo -e "${RED}⚠️  Warning: Less than 8GB RAM available${NC}"
            echo -e "${YELLOW}   Some services may run slowly${NC}"
        fi
    fi
    
    echo -e "${GREEN}✓ Prerequisites check completed${NC}"
}

setup_environment() {
    echo -e "${YELLOW}Setting up environment...${NC}"
    
    # Create .env if it doesn't exist
    if [ ! -f .env ]; then
        echo -e "${YELLOW}Creating .env file from template...${NC}"
        cp .env.example .env
        echo -e "${GREEN}✓ .env file created${NC}"
    fi
    
    # Create necessary directories
    mkdir -p data/postgresql
    mkdir -p data/minio
    mkdir -p data/models
    mkdir -p logs
    
    echo -e "${GREEN}✓ Environment setup completed${NC}"
}

start_basic_stack() {
    echo -e "${GREEN}🚀 Starting basic stack (Projects 1-3)...${NC}"
    docker-compose up -d postgres minio zookeeper kafka spark-master spark-worker
    sleep 10
    docker-compose up -d kafka-producer spark-job
    docker-compose up -d airflow-webserver airflow-scheduler dbt-runner
    docker-compose up -d model-api superset
    
    echo -e "${GREEN}✅ Basic stack started successfully!${NC}"
    show_service_urls
}

start_with_flink() {
    echo -e "${YELLOW}⚡ Starting platform with Apache Flink...${NC}"
    start_basic_stack
    docker-compose up -d flink-jobmanager flink-taskmanager
    sleep 10
    docker-compose --profile flink up -d
    
    echo -e "${GREEN}✅ Platform with Flink started successfully!${NC}"
    echo -e "${CYAN}🔗 Flink Web UI: http://localhost:8081${NC}"
}

start_with_hadoop() {
    echo -e "${YELLOW}🐘 Starting platform with Hadoop...${NC}"
    start_basic_stack
    docker-compose up -d namenode datanode yarn-resourcemanager yarn-nodemanager hive-metastore
    sleep 20
    docker-compose --profile hadoop up -d
    
    echo -e "${GREEN}✅ Platform with Hadoop started successfully!${NC}"
    echo -e "${CYAN}🔗 Hadoop NameNode: http://localhost:9870${NC}"
    echo -e "${CYAN}🔗 YARN ResourceManager: http://localhost:8088${NC}"
}

start_with_bigdata() {
    echo -e "${YELLOW}📊 Starting platform with Big Data generators...${NC}"
    start_basic_stack
    docker-compose --profile bigdata up -d
    
    echo -e "${GREEN}✅ Platform with Big Data generators started successfully!${NC}"
}

start_full_platform() {
    echo -e "${PURPLE}🌟 Starting FULL platform (all components)...${NC}"
    echo -e "${YELLOW}This may take several minutes...${NC}"
    
    # Start core infrastructure
    docker-compose up -d postgres minio zookeeper kafka
    sleep 15
    
    # Start Hadoop ecosystem
    docker-compose up -d namenode datanode yarn-resourcemanager yarn-nodemanager hive-metastore
    sleep 20
    
    # Start Flink
    docker-compose up -d flink-jobmanager flink-taskmanager
    sleep 10
    
    # Start Spark
    docker-compose up -d spark-master spark-worker
    sleep 10
    
    # Start applications
    docker-compose up -d kafka-producer spark-job
    docker-compose up -d airflow-webserver airflow-scheduler dbt-runner
    docker-compose up -d model-api superset
    
    # Start profiles
    docker-compose --profile flink --profile hadoop --profile bigdata up -d
    
    echo -e "${GREEN}🎉 FULL platform started successfully!${NC}"
    show_all_urls
}

show_service_urls() {
    echo -e "\n${CYAN}🌐 Web Interfaces Available:${NC}"
    echo -e "${GREEN}📊 Apache Superset:${NC}     http://localhost:8088 (admin/admin)"
    echo -e "${GREEN}🔄 Apache Airflow:${NC}      http://localhost:8085 (admin/admin)" 
    echo -e "${GREEN}💾 MinIO Console:${NC}       http://localhost:9001 (gemini/claude)"
    echo -e "${GREEN}🚀 Spark UI:${NC}            http://localhost:8080"
    echo -e "${GREEN}🤖 ML API Docs:${NC}         http://localhost:8000/docs"
}

show_all_urls() {
    show_service_urls
    echo -e "${YELLOW}⚡ Apache Flink:${NC}        http://localhost:8081"
    echo -e "${YELLOW}🐘 Hadoop NameNode:${NC}     http://localhost:9870"
    echo -e "${YELLOW}📊 YARN ResourceMgr:${NC}    http://localhost:8088"
}

show_status() {
    echo -e "${CYAN}📈 Platform Status:${NC}\n"
    docker-compose ps
}

show_logs() {
    service=${2:-""}
    if [ -z "$service" ]; then
        echo -e "${YELLOW}📋 Available services for logs:${NC}"
        docker-compose ps --services
        echo -e "${CYAN}Usage: $0 logs [service_name]${NC}"
    else
        docker-compose logs -f "$service"
    fi
}

stop_platform() {
    echo -e "${RED}🛑 Stopping platform...${NC}"
    docker-compose --profile flink --profile hadoop --profile bigdata down
    echo -e "${GREEN}✅ Platform stopped${NC}"
}

clean_platform() {
    echo -e "${RED}🧹 Cleaning up platform data...${NC}"
    read -p "This will remove all data and containers. Continue? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose --profile flink --profile hadoop --profile bigdata down -v --remove-orphans
        docker system prune -f
        rm -rf data/postgresql/* data/minio/* logs/*
        echo -e "${GREEN}✅ Platform cleaned${NC}"
    else
        echo -e "${YELLOW}Cleanup cancelled${NC}"
    fi
}

rebuild_platform() {
    echo -e "${YELLOW}🔨 Rebuilding platform...${NC}"
    docker-compose --profile flink --profile hadoop --profile bigdata down
    docker-compose build --no-cache
    echo -e "${GREEN}✅ Platform rebuilt${NC}"
}

open_web_interfaces() {
    echo -e "${CYAN}🌐 Opening web interfaces...${NC}"
    
    # Check if running on macOS or Linux
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open_cmd="open"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        open_cmd="xdg-open"
    else
        echo -e "${YELLOW}Please manually open the URLs shown above${NC}"
        return
    fi
    
    # Open main interfaces
    $open_cmd "http://localhost:8088" 2>/dev/null &  # Superset
    sleep 2
    $open_cmd "http://localhost:8085" 2>/dev/null &  # Airflow
    sleep 2
    $open_cmd "http://localhost:8080" 2>/dev/null &  # Spark
    sleep 2
    $open_cmd "http://localhost:9001" 2>/dev/null &  # MinIO
    
    echo -e "${GREEN}✅ Web interfaces opened${NC}"
}

run_tests() {
    echo -e "${YELLOW}🧪 Running platform tests...${NC}"
    
    # Test basic connectivity
    echo "Testing service connectivity..."
    
    services=(
        "postgres:5432"
        "kafka:9092" 
        "spark-master:7077"
        "minio:9000"
    )
    
    for service in "${services[@]}"; do
        if docker-compose exec -T "${service%%:*}" echo "OK" 2>/dev/null; then
            echo -e "${GREEN}✓ ${service}${NC}"
        else
            echo -e "${RED}✗ ${service}${NC}"
        fi
    done
    
    # Test web interfaces
    echo "Testing web interfaces..."
    
    urls=(
        "http://localhost:8088/health"
        "http://localhost:8085/health" 
        "http://localhost:8080"
        "http://localhost:9001"
    )
    
    for url in "${urls[@]}"; do
        if curl -f -s "$url" >/dev/null 2>&1; then
            echo -e "${GREEN}✓ ${url}${NC}"
        else
            echo -e "${RED}✗ ${url}${NC}"
        fi
    done
    
    echo -e "${GREEN}✅ Tests completed${NC}"
}

# Main script logic
case "${1:-help}" in
    start)
        print_banner
        check_prerequisites
        setup_environment
        start_basic_stack
        ;;
    flink)
        print_banner
        check_prerequisites
        setup_environment
        start_with_flink
        ;;
    hadoop)
        print_banner
        check_prerequisites
        setup_environment
        start_with_hadoop
        ;;
    bigdata)
        print_banner
        check_prerequisites
        setup_environment
        start_with_bigdata
        ;;
    full)
        print_banner
        check_prerequisites
        setup_environment
        start_full_platform
        ;;
    stop)
        stop_platform
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs "$@"
        ;;
    clean)
        clean_platform
        ;;
    rebuild)
        rebuild_platform
        ;;
    ui)
        open_web_interfaces
        ;;
    test)
        run_tests
        ;;
    help|*)
        print_banner
        show_help
        ;;
esac
