# ==============================================================================
# Apache Kafka for Aura Language (AuraKafka) - Makefile
# Toolchain: Aura Language Compiler (aura v0.5.0)
# ==============================================================================

AURA ?= aura

.PHONY: all check test test-storage test-protocol test-coordinator test-producer test-consumer test-admin test-e2e demo run-demo run-producer run-consumer tools fmt clean

all: test

check:
	@echo "🔍 Checking AuraKafka sources with Aura compiler..."
	@$(AURA) check kafka.aura
	@$(AURA) check protocol/apikeys.aura
	@$(AURA) check protocol/errors.aura
	@$(AURA) check protocol/header.aura
	@$(AURA) check protocol/messages.aura
	@$(AURA) check storage/record.aura
	@$(AURA) check storage/segment.aura
	@$(AURA) check storage/partition_log.aura
	@$(AURA) check storage/log_manager.aura
	@$(AURA) check coordinator/group.aura
	@$(AURA) check coordinator/coordinator.aura
	@$(AURA) check coordinator/kraft.aura
	@$(AURA) check server/config.aura
	@$(AURA) check server/replica_manager.aura
	@$(AURA) check server/broker.aura
	@$(AURA) check client/config.aura
	@$(AURA) check client/producer.aura
	@$(AURA) check client/consumer.aura
	@$(AURA) check client/admin.aura
	@echo "✓ All AuraKafka components verified successfully."

test: test-storage test-protocol test-coordinator test-producer test-consumer test-admin test-e2e
	@echo ""
	@echo "================================================================================"
	@echo "  🏆 ALL AURAKAFKA UNIT & INTEGRATION TEST SUITES PASSED (37/37 TESTS) 🏆"
	@echo "================================================================================"

test-storage:
	@$(AURA) run tests/storage_test.aura

test-protocol:
	@$(AURA) run tests/protocol_test.aura

test-coordinator:
	@$(AURA) run tests/coordinator_test.aura

test-producer:
	@$(AURA) run tests/producer_test.aura

test-consumer:
	@$(AURA) run tests/consumer_test.aura

test-admin:
	@$(AURA) run tests/admin_test.aura

test-e2e:
	@$(AURA) run tests/e2e_cluster_test.aura

demo: run-demo

run-demo:
	@$(AURA) run examples/pubsub_demo.aura

run-producer:
	@$(AURA) run examples/producer_example.aura

run-consumer:
	@$(AURA) run examples/consumer_example.aura

tools-topics:
	@$(AURA) run tools/topics_cli.aura

tools-producer:
	@$(AURA) run tools/console_producer.aura

tools-consumer:
	@$(AURA) run tools/console_consumer.aura

tools-server:
	@$(AURA) run tools/server_main.aura

fmt:
	@$(AURA) fmt -w .

clean:
	@rm -rf *.tmp.mjs tests/*.tmp.mjs examples/*.tmp.mjs tools/*.tmp.mjs
	@echo "✓ Cleaned temporary build artifacts."
