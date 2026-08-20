"""wkafka-mcp: Model Context Protocol server for WKafka architecting."""

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
from functools import lru_cache

from mcp.server.fastmcp import FastMCP

from wkafka_mcp.catalog import PatternsCatalog
from wkafka_mcp.templates import TemplateGenerator

# Setup logging strictly to stderr to avoid breaking MCP protocol
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stderr
)
logger = logging.getLogger(__name__)

# PID file for background service
PID_FILE = os.path.expanduser("~/.wkafka_mcp.pid")

# Create the primary FastMCP Server instance
mcp = FastMCP("wkafka-mcp-server")


@lru_cache(maxsize=1)
def get_catalog() -> PatternsCatalog:
    """Return the lazily-initialized shared catalog instance."""
    return PatternsCatalog()


@mcp.tool()
def get_wkafka_architect_blueprints() -> str:
    """Complete reference with production-ready WKafka patterns and examples."""
    basic_code = (
        "from wkafka import WKafka\n"
        "\n"
        'kafka = WKafka(bootstrap_servers="localhost:9092")\n'
        "\n"
        '@kafka.consumer(topic="orders", format="json")\n'
        "def handle_order(msg):\n"
        '    print(f"New order: {msg.value}")\n'
        "\n"
        "kafka.run_consumers(block=True)\n"
        "\n"
        "with kafka.producer() as p:\n"
        '    p.send("orders", value={"id": 123}, key="order-123", format="json")\n'
    )

    image_code = (
        "import cv2\n"
        "from wkafka import WKafka\n"
        "from wkafka.serializers import ImageSerializer\n"
        "\n"
        "kafka = WKafka()\n"
        "serializer = ImageSerializer()\n"
        "\n"
        '@kafka.consumer(topic="camera-frames", format="image")\n'
        "def handle_frame(msg):\n"
        "    # msg.value is a numpy.ndarray (BGR frame)\n"
        '    cv2.imshow("frame", msg.value)\n'
        "\n"
        "with kafka.producer() as p:\n"
        '    frame = cv2.imread("image.jpg")\n'
        '    p.send("camera-frames", value=frame, format="image", quality=85)\n'
    )

    yaml_code = (
        "from wkafka import WKafka\n"
        "\n"
        "kafka = WKafka()\n"
        "\n"
        '@kafka.consumer(topic="config-updates", format="yaml")\n'
        "def handle_config(msg):\n"
        '    print(f"Applied config: {msg.value}")\n'
        "\n"
        "with kafka.producer() as p:\n"
        '    p.send("config-updates", value={"feature": "dark_mode", "enabled": True}, format="yaml")\n'
    )

    sasl_code = (
        "from wkafka import WKafka\n"
        "\n"
        "kafka = WKafka(\n"
        '    bootstrap_servers="my-broker:9092",\n'
        '    security_protocol="SASL_PLAINTEXT",\n'
        '    sasl_mechanism="SCRAM-SHA-512",\n'
        '    sasl_plain_username="admin",\n'
        '    sasl_plain_password="password",\n'
        ")\n"
        "\n"
        '@kafka.consumer(topic="secure-topic", format="json")\n'
        "def handle_secure(msg):\n"
        '    print(f"Secure message: {msg.value}")\n'
    )

    headers_code = (
        "from wkafka import WKafka\n"
        "\n"
        "kafka = WKafka()\n"
        "\n"
        "with kafka.producer() as p:\n"
        "    p.send(\n"
        '        "orders",\n'
        '        value={"id": 456},\n'
        '        headers={"trace_id": "abc123", "tenant": "acme"},\n'
        '        format="json",\n'
        "    )\n"
    )

    keyfilter_code = (
        "from wkafka import WKafka\n"
        "\n"
        "kafka = WKafka()\n"
        "\n"
        '@kafka.consumer(topic="events", key_filter="payments", format="json")\n'
        "def handle_payments(msg):\n"
        '    # Only messages with key == "payments" reach this handler\n'
        '    print(f"Payment: {msg.value}")\n'
    )

    legacy_code = (
        "from wkafka.controller.wkafka import Wkafka\n"
        "\n"
        "# Legacy-compatible bridge: server/name/retry_delay/max_retries\n"
        'kafka = Wkafka(server="localhost:9092", name="legacy-app", max_retries=3)\n'
        "\n"
        '@kafka.consumer(topic="orders", value_type="json")\n'
        "def handle(msg):\n"
        '    print(f"Received: {msg.value}")\n'
        "\n"
        'kafka.send("orders", value={"id": 1}, key="k1")\n'
    )

    return (
        "WKAFKA EXPERT BLUEPRINTS (PRODUCTION REFERENCE - READ/WRITE FOR EVERY SERIALIZATION)\n\n"
        "=== 1. BASIC JSON PRODUCER/CONSUMER ===\n" + basic_code + "\n"
        "=== 2. IMAGE / VISION PIPELINE ===\n" + image_code + "\n"
        "=== 3. YAML CONFIGURATION ===\n" + yaml_code + "\n"
        "=== 4. SASL AUTHENTICATION ===\n" + sasl_code + "\n"
        "=== 5. HEADERS (METADATA / TRACING) ===\n" + headers_code + "\n"
        "=== 6. KEY FILTERED CONSUMPTION ===\n" + keyfilter_code + "\n"
        "=== 7. LEGACY CONTROLLER BRIDGE ===\n" + legacy_code
    )


@mcp.tool()
def get_wkafka_architect_manual() -> str:
    """Expert manual for building high-performance Kafka systems (wisrovi standard)."""
    return (
        "WKAFKA ARCHITECT MANUAL (ADVANCED)\n"
        "--- PROJECT STRUCTURE RULES (MANDATORY) ---\n"
        "1. CONFIG: All Kafka settings MUST be centralized in a config file or environment variables. Prefer environment variables for credentials.\n"
        "2. MODELS: Message schemas SHOULD be defined as Pydantic models or dataclasses and serialized with format='json'.\n"
        "3. SERIALIZERS: All message values MUST use the extensible serializer system (json, yaml, image supported out-of-the-box).\n"
        "4. CONTROLLER: All consumer logic MUST be placed in dedicated handler functions decorated with @kafka.consumer(topic, format=...).\n"
        "5. ORCHESTRATOR: The service entrypoint MUST use the WKafka instance and context-manager pattern (with kafka.producer()).\n"
        "6. SECURITY: SASL credentials MUST be loaded from environment variables, never hardcoded.\n"
        "7. MONITORING: Always implement error handling in consumer callbacks; log all exceptions via loguru.\n\n"
        "--- CORE RULES ---\n"
        "1. Always use the serializer `format` parameter on consumer/producer calls: 'json', 'yaml', or 'image'.\n"
        "2. Prefer `with kafka.producer() as p:` for context-manager-safe production (flush + close on exit).\n"
        "3. Set `auto_offset_reset` appropriately: 'earliest' for replay, 'latest' for new consumers.\n"
        "4. Keep `enable_auto_commit=True` for normal operation.\n"
        "5. Configure `acks` via the KAFKA_ACKS env var: 0=fire-and-forget, 1=leader-write, -1/all=full-guarantee.\n"
        "6. For SASL/SSL, always set `security_protocol` along with the corresponding `sasl_mechanism`.\n"
        "7. Use `key` for ordering guarantees on the same partition and `key_filter` on consumers to partition work.\n"
        "8. Use `headers` for message metadata and tracing context propagation.\n"
        "9. For production, set a unique `client_id`; enable `dynamic_group_id=True` when each run needs a fresh consumer group.\n"
        "10. Install `wkafka[snappy]` for snappy compression; wkafka falls back to gzip automatically.\n\n"
        "--- DATA FLOW GUIDE (WHEN TO USE WHAT) ---\n"
        "NEED basic JSON messages? -> WKafka() + @kafka.consumer(topic, format='json') + p.send(topic, value=..., format='json')\n"
        "NEED multimedia (images/video frames)? -> WKafka() + format='image' (NumPy arrays / PIL images, JPEG-encoded).\n"
        "NEED YAML configuration payloads? -> WKafka() + format='yaml'.\n"
        "NEED SASL authentication? -> WKafka(security_protocol='SASL_PLAINTEXT', sasl_mechanism='SCRAM-SHA-512', sasl_plain_username=..., sasl_plain_password=...).\n"
        "NEED KRaft mode (no Zookeeper)? -> WKafka with bootstrap_servers pointing to KRaft controllers.\n"
        "NEED only certain keys processed? -> @kafka.consumer(topic, key_filter='my-key').\n"
        "NEED multiple consumers concurrently? -> Decorate several handlers and call kafka.run_consumers(block=True) (one thread per consumer).\n"
        "NEED tracing/metadata? -> pass headers={'trace_id': '...', 'tenant': '...'} on send().\n"
        "NEED a fresh consumer group per run? -> WKafka(dynamic_group_id=True).\n"
        "NEED custom serialization (Avro/Protobuf)? -> Subclass wkafka.serializers.base.Serializer and register it.\n"
        "NEED legacy compatibility with the old Wkafka API? -> from wkafka.controller.wkafka import Wkafka (server/name/retry_delay/max_retries).\n\n"
        "--- MODULE MAP (every public entry point) ---\n"
        "wkafka                     -> WKafka, Message, Wkafka (alias)\n"
        "wkafka.core.manager        -> WKafka (consumer/send/producer/run_consumers/__enter__/__exit__)\n"
        "wkafka.core.models         -> Message (frozen dataclass: value, key, topic, group_id, headers, offset)\n"
        "wkafka.serializers.base    -> Serializer, JSONSerializer, YAMLSerializer, ImageSerializer\n"
        "wkafka.serializers         -> re-exports the serializer classes\n"
        "wkafka.controller.wkafka   -> Wkafka (legacy bridge with server/name/retry_delay/max_retries/value_type)\n\n"
        "--- REFACTORING A MONOLITH TO WKAFKA ---\n"
        "When refactoring a monolithic script into a WKafka-based service, follow this exact workflow:\n"
        "Step 1: Identify the events/commands that cross module boundaries and give each one a typed schema.\n"
        "Step 2: Create `config/settings.py` centralizing bootstrap servers, client_id, acks, and SASL env vars.\n"
        "Step 3: For each consumer concern, create one handler in `consumers/` decorated with @kafka.consumer(topic, format=...).\n"
        "Step 4: Create producers in `producers/` using `with kafka.producer() as p:`.\n"
        "Step 5: Create `main.py` that instantiates WKafka, imports consumers (so decorators register), and calls run_consumers(block=True).\n"
        "Step 6: Generate a professional, intuitive, and modern `README.md` (in English) documenting what the newly created service does. You MUST include a Mermaid flowchart diagram (`mermaid`) illustrating the data flow between topics, producers, and consumers. Also, you MUST include a footer or header stating: 'Generated by WKafka MCP by wisrovi'.\n\n"
        "--- WPIPE & WKAFKA INTEGRATION (MICROSERVICES STANDARD) ---\n"
        "To build a structured pipeline worker microservice with WKafka, follow this exact structure:\n"
        "```python\n"
        "import sys\n"
        "import os\n"
        "from wkafka import WKafka\n\n"
        "# Ensure local imports\n"
        "sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))\n"
        "sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils', 'eyesnroad_tools')))\n\n"
        "from config.settings import KafkaSettings\n"
        "from dto.context import HorizontalesContext\n\n"
        "kafka = WKafka(bootstrap_servers=KafkaSettings.bootstrap_servers, client_id=KafkaSettings.client_id)\n\n"
        "@kafka.consumer(topic=KafkaSettings.topic_name, format='json')\n"
        "def worker(msg):\n"
        "    record = msg.value\n"
        "    context = HorizontalesContext(\n"
        "        token=record.get('token', ''),\n"
        "        images=record.get('images', ''),\n"
        "        output=record.get('output', ''),\n"
        "        country_name=record.get('COUNTRY_NAME', record.get('country_name', 'spain')),\n"
        "        local_run=record.get('local_run', False),\n"
        "        yolo_repo_path=record.get('yolo_repo_path', '../001-cnn-diurnas')\n"
        "    )\n"
        "    print(f'Task received for token: {context.token}')\n"
        "    print('pipeline run')\n\n"
        "if __name__ == '__main__':\n"
        "    print('Starting Horizontales Microservice with WKafka...')\n"
        "    kafka.run_consumers(block=True)\n"
        "```"
    )


@mcp.tool()
def search_wkafka_pattern(query: str) -> str:
    """Search for production-ready Kafka patterns in official and community catalogs."""
    results = get_catalog().search(query)
    if not results:
        return f"No pattern matching '{query}' was found. Recommend building a native WKafka pattern with the corresponding manager."

    response = "Found production-ready architectural patterns in wisrovi SUITE:\n\n"
    for p in results:
        response += f"🚀 [{p.get('origin', 'Unknown')}] {p.get('name', 'N/A')}\n"
        response += f"   - Manager: {p.get('manager', 'N/A')}\n"
        response += f"   - Module: {p.get('module', 'N/A')}\n"
        response += f"   - Description: {p.get('description', 'N/A')}\n\n"
    return response


@mcp.tool()
def deploy_wkafka_scaffolding(
    target_dir: str,
    project_name: str = "wkafka_project",
    scaffold_type: str = "standard",
) -> str:
    """Deploys a professional WKafka project structure following wisrovi standards."""
    try:
        if not os.path.isabs(target_dir):
            return "Error: target_dir must be an absolute path."

        for folder in TemplateGenerator.get_folders(scaffold_type):
            os.makedirs(os.path.join(target_dir, folder), exist_ok=True)

        blueprints = TemplateGenerator.get_files_blueprint(scaffold_type, project_name)
        for rel_path, content in blueprints.items():
            full_path = os.path.join(target_dir, rel_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(content)

        return f"Success: WKafka architecture '{project_name}' deployed at {target_dir}"
    except (OSError, PermissionError, FileNotFoundError) as e:
        return f"Error: {str(e)}"


@mcp.tool()
def generate_from_pattern(
    pattern_name: str, target_dir: str, project_name: str = "wkafka_app"
) -> str:
    """Generate a complete project from a specific catalog pattern.

    Fetches the pattern details and deploys a tailored project structure
    with the appropriate manager, configuration, and example usage.
    """
    try:
        if not os.path.isabs(target_dir):
            return "Error: target_dir must be an absolute path."

        catalog = get_catalog()
        patterns = catalog.search(pattern_name)
        if not patterns:
            return f"Error: Pattern '{pattern_name}' not found. Use search_wkafka_pattern to find available patterns."

        pattern = patterns[0]

        for folder in TemplateGenerator.get_folders("standard"):
            os.makedirs(os.path.join(target_dir, folder), exist_ok=True)

        blueprints = TemplateGenerator.get_files_blueprint("standard", project_name)
        for rel_path, content in blueprints.items():
            full_path = os.path.join(target_dir, rel_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(content)

        example_path = os.path.join(target_dir, "examples", f"{pattern_name}.py")
        os.makedirs(os.path.dirname(example_path), exist_ok=True)
        with open(example_path, "w", encoding="utf-8") as f:
            f.write(_generate_pattern_example(pattern, project_name))

        return (
            f"Success: Project '{project_name}' generated from pattern '{pattern_name}' at {target_dir}\n"
            f"  Pattern: {pattern.get('name')}\n"
            f"  Manager: {pattern.get('manager')}\n"
            f"  Module: {pattern.get('module')}\n"
            f"  Example: examples/{pattern_name}.py"
        )
    except (OSError, PermissionError, FileNotFoundError) as e:
        return f"Error: {str(e)}"


def _generate_pattern_example(pattern: dict, project_name: str) -> str:
    """Generate a pattern-specific example file."""
    name = pattern.get("name", "pattern")
    manager = pattern.get("manager", "WKafka")
    module = pattern.get("module", "wkafka")
    desc = pattern.get("description", "")

    return (
        f"# Example: {name}\n"
        f"# {desc}\n"
        f"# Generated from WKafka pattern catalog\n\n"
        f"from {module} import {manager}\n\n"
        f"def main():\n"
        f"    # TODO: Add pattern-specific usage\n"
        f"    pass\n\n"
        f'if __name__ == "__main__":\n'
        f"    main()\n"
    )


@mcp.tool()
def validate_kafka_config(config_code: str) -> str:
    """Validate a Kafka configuration snippet for common issues.

    Checks for missing bootstrap_servers, suspicious hardcoded credentials,
    and obvious malformed configuration.
    """
    if not config_code or not config_code.strip():
        return "❌ Empty configuration code"

    checks = []
    if "bootstrap_servers" not in config_code:
        checks.append("⚠️ Missing bootstrap_servers configuration")

    for secret_word in ("password=", "password =", 'sasl_plain_password="', "secret"):
        if (
            secret_word in config_code
            and "os.environ" not in config_code
            and "getenv" not in config_code
        ):
            checks.append(
                "⚠️ Hardcoded credential detected - load secrets from environment variables"
            )
            break

    if not checks:
        return "✅ Configuration validated successfully"
    return "\n".join(checks)


@mcp.tool()
def generate_wkafka_consumer(
    topic: str,
    format: str = "json",
    key_filter: str = None,
    target_file: str = None,
) -> str:
    """Generate production-ready WKafka consumer boilerplate code.

    Args:
        topic: The Kafka topic to subscribe to.
        format: Serialization format (json, yaml, or image).
        key_filter: Optional filter to consume only messages matching this key.
        target_file: Optional absolute path to write the generated code directly to.
    """
    key_filter_str = f', key_filter="{key_filter}"' if key_filter else ""
    code = (
        "from wkafka import WKafka\n"
        "from config.settings import BOOTSTRAP_SERVERS, CLIENT_ID\n\n"
        "# Initialize WKafka consumer with connection settings\n"
        "kafka = WKafka(bootstrap_servers=BOOTSTRAP_SERVERS, client_id=CLIENT_ID)\n\n"
        f'@kafka.consumer(topic="{topic}", format="{format}"{key_filter_str}, auto_offset_reset="earliest")\n'
        f"def handle_{topic.replace('-', '_')}(msg):\n"
        '    """Consumer callback for processing messages."""\n'
        '    # msg.value contains the deserialized payload\n'
        '    print(f"Received message from topic {topic}: {msg.value} (key: {msg.key}, offset: {msg.offset})")\n\n'
        'if __name__ == "__main__":\n'
        '    print("Starting consumers... Press Ctrl+C to stop.")\n'
        '    kafka.run_consumers(block=True)\n'
    )

    if target_file:
        try:
            if not os.path.isabs(target_file):
                return "Error: target_file must be an absolute path."
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            with open(target_file, "w", encoding="utf-8") as f:
                f.write(code)
            return f"Success: Consumer code written to {target_file}"
        except OSError as e:
            return f"Error writing file: {str(e)}"

    return code


@mcp.tool()
def generate_wkafka_producer(
    topic: str,
    format: str = "json",
    target_file: str = None,
) -> str:
    """Generate production-ready WKafka producer boilerplate code.

    Args:
        topic: The Kafka topic to send messages to.
        format: Serialization format (json, yaml, or image).
        target_file: Optional absolute path to write the generated code directly to.
    """
    code = (
        "from wkafka import WKafka\n"
        "from config.settings import BOOTSTRAP_SERVERS, CLIENT_ID, ACKS\n\n"
        "# Initialize WKafka producer with connection settings\n"
        "kafka = WKafka(bootstrap_servers=BOOTSTRAP_SERVERS, client_id=CLIENT_ID, acks=ACKS)\n\n"
        "def send_message(payload, key=None):\n"
        '    """Sends a single message safely using the producer context manager."""\n'
        "    with kafka.producer() as p:\n"
        f'        p.send("{topic}", value=payload, key=key, format="{format}")\n'
        '        print(f"Message sent to {topic}")\n\n'
        'if __name__ == "__main__":\n'
        '    # Example payload\n'
        '    example_data = {"status": "ok", "message": "hello from wkafka producer"}\n'
        '    send_message(example_data, key="test-key")\n'
    )

    if target_file:
        try:
            if not os.path.isabs(target_file):
                return "Error: target_file must be an absolute path."
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            with open(target_file, "w", encoding="utf-8") as f:
                f.write(code)
            return f"Success: Producer code written to {target_file}"
        except OSError as e:
            return f"Error writing file: {str(e)}"

    return code


# --- CLI Actions ---


def run_stdio():
    """Runs the MCP server in stdio mode (standard for agents)."""
    mcp.run(transport="stdio")


def run_sse():
    """Runs the MCP server in SSE mode."""
    mcp.run(transport="sse")


def start_background():
    """Starts the SSE server in the background."""
    if os.path.exists(PID_FILE):
        print("Server is already running or PID file exists.")
        return

    with (
        open(os.path.expanduser("~/wkafka_mcp.log"), "a", encoding="utf-8") as log_file,
        subprocess.Popen(
            [sys.executable, "-m", "wkafka_mcp.server", "run-sse"],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        ) as proc,
        open(PID_FILE, "w", encoding="utf-8") as f,
    ):
        f.write(str(proc.pid))
    print(f"wkafka-mcp started in background (SSE mode) with PID {proc.pid}")


def stop_background():
    """Stops the background SSE server."""
    if not os.path.exists(PID_FILE):
        print("No background server running.")
        return

    with open(PID_FILE, encoding="utf-8") as f:
        pid = int(f.read())

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Stopped server with PID {pid}")
    except ProcessLookupError:
        print("Process not found.")
    finally:
        os.remove(PID_FILE)


def print_config(write_file: bool = True):
    """Prints or saves the JSON configuration for agents."""
    python_path = sys.executable
    config = {
        "mcpServers": {
            "wkafka-mcp": {
                "command": python_path,
                "args": ["-m", "wkafka_mcp.server", "run"],
                "env": {},
            }
        }
    }

    config_json = json.dumps(config, indent=2)

    helper_text = (
        "\n=========================================\n"
        "🔌 QUICK INSTALL COMMANDS FOR AI AGENTS\n"
        "=========================================\n\n"
        "For Gemini CLI:\n"
        f"  gemini mcp add wkafka-mcp {python_path} -m wkafka_mcp.server run\n\n"
        "For Claude Desktop / Cursor:\n"
        "  Copy the JSON above (or from the saved file) into your agent's config file.\n"
        "=========================================\n"
    )

    if not write_file:
        print(config_json)
        print(helper_text)
        return

    target_dir = os.getcwd()
    agents_dir = os.path.join(target_dir, ".agents")
    os.makedirs(agents_dir, exist_ok=True)

    config_path = os.path.join(agents_dir, "wkafka-mcp.json")
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(config_json)

    print(f"✅ Configuration saved to: {config_path}")
    print(helper_text)


# --- Main Entry Point ---


def main():
    """Parse CLI arguments and dispatch to the requested command."""
    parser = argparse.ArgumentParser(
        description="wkafka-mcp: WKafka Architect MCP Server"
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run", "run-sse", "start", "stop", "config", "help"],
        help="Command to execute (default: run)",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print configuration to stdout instead of saving to .agents/",
    )

    args = parser.parse_args()

    if args.command == "config":
        logging.getLogger().setLevel(logging.ERROR)
        print_config(write_file=not args.print)
        return

    if args.command == "run":
        run_stdio()
    elif args.command == "run-sse":
        run_sse()
    elif args.command == "start":
        start_background()
    elif args.command == "stop":
        stop_background()
    elif args.command == "help":
        parser.print_help()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
