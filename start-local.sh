#!/usr/bin/env bash
# ==============================================================================
# AuraKafka - Local Development & Service Initialization Script
# Toolchain: Aura Language Compiler (aura)
# ==============================================================================

set -euo pipefail

# Text formatting
BOLD="\033[1m"
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[0;33m"
CYAN="\033[0;36m"
RED="\033[0;31m"
RESET="\033[0m"

AURA_BIN="${AURA:-aura}"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASE_DIR"

print_header() {
  echo -e "${CYAN}${BOLD}"
  echo "    _                 _  __        __ _          "
  echo "   / \   _   _ _ __ __ _| |/ /__ _ / _| | ____ _ "
  echo "  / _ \ | | | | '__/ _\` | ' // _\` | |_| |/ / _\` |"
  echo " / ___ \| |_| | | | (_| | . \ (_| |  _|   < (_| |"
  echo "/_/   \_\\__,_|_|  \__,_|_|\_\__,_|_| |_|\_\__,_|"
  echo " Apache Kafka on Aura Language (Local Environment)"
  echo -e "==================================================${RESET}"
}

check_prerequisites() {
  echo -e "${BLUE}🔍 Verificando requisitos del entorno...${RESET}"
  if ! command -v "$AURA_BIN" &> /dev/null; then
    echo -e "${RED}✕ Error: Compilador de Aura ('$AURA_BIN') no encontrado en el PATH.${RESET}"
    echo "  Por favor instala la herramienta Aura CLI (https://github.com/aurarepo/aura)."
    exit 1
  fi

  AURA_VER=$("$AURA_BIN" version 2>/dev/null || echo "Desconocida")
  echo -e "  ${GREEN}✓${RESET} Compilador Aura detectado: ${BOLD}${AURA_VER}${RESET}"
}

init_directories() {
  mkdir -p "$BASE_DIR/bin"
  mkdir -p "$BASE_DIR/data"
}

run_server() {
  echo -e "\n${BLUE}${BOLD}[1/4] Iniciando Broker Server de Kafka (Modo KRaft)...${RESET}"
  "$AURA_BIN" run tools/server_main.aura
}

run_topics() {
  echo -e "\n${BLUE}${BOLD}[2/4] Aprovisionando Tópicos con kafka-topics CLI...${RESET}"
  "$AURA_BIN" run tools/topics_cli.aura
}

run_producer() {
  echo -e "\n${BLUE}${BOLD}[3/4] Publicando Mensajes con kafka-console-producer...${RESET}"
  "$AURA_BIN" run tools/console_producer.aura
}

run_consumer() {
  echo -e "\n${BLUE}${BOLD}[4/4] Consumiendo Mensajes con kafka-console-consumer...${RESET}"
  "$AURA_BIN" run tools/console_consumer.aura
}

run_demo() {
  echo -e "\n${BLUE}${BOLD}🚀 Ejecutando Pipeline Distribuido Pub/Sub End-to-End...${RESET}"
  "$AURA_BIN" run examples/pubsub_demo.aura
}

run_bench() {
  echo -e "\n${BLUE}${BOLD}⚡ Ejecutando Micro-Benchmark de Rendimiento...${RESET}"
  "$AURA_BIN" run tests/benchmark.aura
}

run_tests() {
  echo -e "\n${BLUE}${BOLD}🧪 Ejecutando Suite de Pruebas (37/37)...${RESET}"
  make test
}

run_check() {
  echo -e "\n${BLUE}${BOLD}🔍 Verificando Fuentes con el Compilador Aura...${RESET}"
  make check
}

run_clean() {
  echo -e "\n${YELLOW}🧹 Limpiando temporales y binarios...${RESET}"
  make clean
  rm -rf "$BASE_DIR/bin" "$BASE_DIR/data"
  echo -e "${GREEN}✓ Limpieza completada.${RESET}"
}

run_all() {
  print_header
  check_prerequisites
  init_directories

  echo -e "\n${YELLOW}${BOLD}=== INICIANDO FLUJO COMPLETO DE SERVICIOS LOCALES ===${RESET}"
  run_server
  run_topics
  run_producer
  run_consumer

  echo -e "\n${GREEN}${BOLD}======================================================${RESET}"
  echo -e "${GREEN}${BOLD}  ✨ TODOS LOS SERVICIOS LOCALES INICIALIZADOS CON ÉXITO${RESET}"
  echo -e "${GREEN}${BOLD}======================================================${RESET}"
  echo -e "Para ejecutar el demo pub/sub interactivo: ${CYAN}./start-local.sh --demo${RESET}"
  echo -e "Para correr benchmarks:                    ${CYAN}./start-local.sh --bench${RESET}"
  echo -e "Para ejecutar las pruebas:                 ${CYAN}./start-local.sh --test${RESET}"
}

print_help() {
  print_header
  echo "Uso: ./start-local.sh [OPCIÓN]"
  echo ""
  echo "Opciones:"
  echo "  (sin args), all, start   Ejecuta el flujo completo de inicialización local"
  echo "  --server, server         Inicia el broker Kafka en modo KRaft (tools/server_main.aura)"
  echo "  --topics, topics         Aprovisiona y lista tópicos (tools/topics_cli.aura)"
  echo "  --producer, producer     Publica mensajes de prueba (tools/console_producer.aura)"
  echo "  --consumer, consumer     Consume mensajes de prueba (tools/console_consumer.aura)"
  echo "  --demo, demo             Ejecuta el pipeline distribuido completo (examples/pubsub_demo.aura)"
  echo "  --bench, bench           Ejecuta el benchmark de throughput (tests/benchmark.aura)"
  echo "  --check, check           Verifica la sintaxis y tipos de todos los módulos"
  echo "  --test, test             Ejecuta las 37 pruebas unitarias e integrales"
  echo "  --clean, clean           Limpia binarios y archivos temporales generados"
  echo "  --help, -h               Muestra este menú de ayuda"
  echo ""
}

# Command dispatch
case "${1:-all}" in
  all|start)
    run_all
    ;;
  --server|server)
    check_prerequisites
    run_server
    ;;
  --topics|topics)
    check_prerequisites
    run_topics
    ;;
  --producer|producer)
    check_prerequisites
    run_producer
    ;;
  --consumer|consumer)
    check_prerequisites
    run_consumer
    ;;
  --demo|demo)
    check_prerequisites
    run_demo
    ;;
  --bench|bench|benchmark)
    check_prerequisites
    run_bench
    ;;
  --check|check)
    check_prerequisites
    run_check
    ;;
  --test|test)
    check_prerequisites
    run_tests
    ;;
  --clean|clean)
    run_clean
    ;;
  --help|-h|help)
    print_help
    ;;
  *)
    echo -e "${RED}Opción desconocida: $1${RESET}"
    print_help
    exit 1
    ;;
esac
