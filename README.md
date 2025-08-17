# StreamFlow Distributed Messaging System

StreamFlow is a complex distributed messaging system designed to evaluate coding agents. It implements a Kafka-like architecture with deep interdependencies that require extensive exploration to understand.

## Architecture

### Core Packages

- **`core/`** - Message, Topic, and TopicPartition abstractions
- **`broker/`** - BrokerNode, ClusterMetadata, and cluster management
- **`partition/`** - Partition management and leader election
- **`replication/`** - Message replication and replica management
- **`storage/`** - Storage engine interfaces and implementations
- **`config/`** - Configuration management (40+ parameters)
- **`metrics/`** - System monitoring and metrics collection
- **`producer/`** - Message production interfaces
- **`consumer/`** - Message consumption interfaces  
- **`admin/`** - Administrative operations

### Key Features

- **Distributed Leadership** - Leader election for controller and partitions
- **Message Replication** - Multi-replica durability with ISR management
- **Dynamic Rebalancing** - Automatic partition redistribution
- **Comprehensive Configuration** - 40+ tunable parameters
- **Monitoring** - Built-in metrics collection and health monitoring

## Building and Testing

```bash
# Compile the project
mvn compile

# Run tests
mvn test

# Package
mvn package
```

## Complexity Features

This codebase is intentionally complex to test agent capabilities:

1. **Cross-Package Dependencies** - Components span 10 packages with intricate interactions
2. **Configuration Cascades** - Single config changes affect multiple components
3. **Distributed Consensus** - Leader election with split-brain prevention
4. **Error Propagation** - Complex failure handling across multiple layers
5. **Performance Tuning** - Multiple bottlenecks requiring careful analysis

## Evaluation Purpose

The codebase serves as a testbed for coding agents to demonstrate:
- Cross-package navigation and understanding
- Configuration impact analysis
- Call graph traversal across abstraction layers
- State transition mapping
- Error path discovery

Typical questions require 25-40 tool calls without pre-computed indices, but could be reduced to 8-15 with intelligent search capabilities - demonstrating 3-5x efficiency improvements.