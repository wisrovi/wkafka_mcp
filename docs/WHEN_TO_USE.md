## wkafka-mcp WHEN TO USE

Use wkafka-mcp when you need:
- Kafka producer/consumer operations with decorator-based API
- Multimedia message support (images, video via OpenCV/NumPy)
- SASL/SSL authentication for secure clusters
- JSON, YAML, and image message formats
- Message validation and serialization
- Consumer group management
- Topic administration (create, list, describe)
- Pattern-based project generation
- SASL PLAIN, SCRAM-SHA-512, PLAIN authentication

### Quick Start
```python
from wkafka_mcp.server import validate_kafka_config, generate_from_pattern

# Validate Kafka configuration
result = validate_kafka_config("""
from kafka import KafkaConsumer, KafkaProducer
config = {{
    "bootstrap_servers": "localhost:9092",
    "group_id": "my-group",
    "auto_offset_reset": "latest"
}}
""")

# Generate project from pattern
project = generate_from_pattern(
    pattern_name="basic_producer_consumer", target_dir="/path/to/project"
)
```
"""
