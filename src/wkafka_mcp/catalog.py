"""Pattern catalog synchronization with local fallbacks for WKafka."""

import json
import logging
from contextlib import suppress
from urllib import request
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)


class PatternsCatalog:
    """Manages the synchronization of available Kafka patterns from the wisrovi SUITE.

    Synchronizes from GitHub just like the VS Code extension (with local fallbacks).
    """

    OFFICIAL_URL = (
        "https://raw.githubusercontent.com/wisrovi/wkafka/main/patterns_catalog.json"
    )
    COMMUNITY_URL = "https://raw.githubusercontent.com/wisrovi/wkafka-plugins/main/patterns_catalog.json"

    def __init__(self):
        """Initialize the catalog with hardcoded offline fallbacks."""
        self.cached_patterns = []
        self._load_initial_catalog()

    def _fetch_url(self, url: str) -> list:
        """Fetch patterns from a URL with timeout and error handling."""
        try:
            req = request.Request(url, headers={"User-Agent": "wkafka-mcp"})
            with request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    return json.loads(response.read().decode("utf-8"))
        except (URLError, HTTPError, TimeoutError, OSError) as e:
            logger.warning("Failed to fetch catalog from %s: %s", url, e)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Failed to fetch catalog from %s: %s", url, e)
        return []

    def refresh_catalog(self) -> list:
        """Fetch latest patterns from both official and community repositories."""
        official = self._fetch_url(self.OFFICIAL_URL)
        community = self._fetch_url(self.COMMUNITY_URL)

        all_patterns = []
        for p in official:
            p["origin"] = "Official"
            all_patterns.append(p)
        for p in community:
            p["origin"] = "Community"
            all_patterns.append(p)

        if all_patterns:
            self.cached_patterns = all_patterns
            logger.info(
                "Catalog refreshed: %d patterns found.", len(self.cached_patterns)
            )

        return self.cached_patterns

    def search(self, query: str) -> list:
        """Filters cataloged patterns based on a search query keyword."""
        if not self.cached_patterns:
            self.refresh_catalog()

        query_lower = query.lower()
        results = []
        for pattern in self.cached_patterns:
            fields = [
                pattern.get("name", ""),
                pattern.get("manager", ""),
                pattern.get("module", ""),
                pattern.get("description", ""),
                pattern.get("category", ""),
            ]
            if any(query_lower in str(f).lower() for f in fields):
                results.append(pattern)
        return results

    def _load_initial_catalog(self):
        """Initial load with hardcoded fallbacks if offline."""
        self.cached_patterns = [
            {
                "name": "basic_json_messaging",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "Producer/consumer JSON messages via @kafka.consumer(topic, format='json')",
                "category": "Core",
                "origin": "Official",
            },
            {
                "name": "yaml_config_messaging",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "YAML-serialized messages for configuration payloads",
                "category": "Serialization",
                "origin": "Official",
            },
            {
                "name": "image_vision_pipeline",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "Send/receive images (NumPy/PIL/OpenCV) via ImageSerializer and format='image'",
                "category": "Multimedia",
                "origin": "Official",
            },
            {
                "name": "sasl_authentication",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "SASL_PLAINTEXT with PLAIN/SCRAM-SHA-512 via security_protocol and sasl_* kwargs",
                "category": "Security",
                "origin": "Official",
            },
            {
                "name": "kramit_mode",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "Kafka without Zookeeper: point bootstrap_servers at KRaft controllers",
                "category": "Deployment",
                "origin": "Official",
            },
            {
                "name": "typed_message_model",
                "manager": "Message",
                "module": "wkafka.core.models",
                "description": "Frozen dataclass with value, key, topic, group_id, offset, headers",
                "category": "Core",
                "origin": "Official",
            },
            {
                "name": "key_filtered_consumption",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "Consumer that only processes messages matching a key via key_filter",
                "category": "Advanced",
                "origin": "Official",
            },
            {
                "name": "multi_consumer_threads",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "run_consumers(block=True) runs each decorated consumer in its own thread",
                "category": "Performance",
                "origin": "Official",
            },
            {
                "name": "metadata_headers",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "Propagate tracing/metadata context with the headers parameter on send()",
                "category": "Observability",
                "origin": "Official",
            },
            {
                "name": "snappy_compression",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "Automatic snappy (or gzip fallback) compression on the producer",
                "category": "Performance",
                "origin": "Official",
            },
            {
                "name": "controller_legacy_bridge",
                "manager": "Wkafka",
                "module": "wkafka.controller.wkafka",
                "description": "Legacy Wkafka class bridging server/name/retry_delay/max_retries to the new WKafka",
                "category": "Compatibility",
                "origin": "Official",
            },
            {
                "name": "structured_loguru_logging",
                "manager": "WKafka",
                "module": "wkafka",
                "description": "loguru rotation to wkafka.log (10 MB) with error tracing in consumers",
                "category": "Observability",
                "origin": "Official",
            },
            {
                "name": "custom_serializer_extension",
                "manager": "Serializer",
                "module": "wkafka.serializers.base",
                "description": "Extend the Serializer ABC for Avro/Protobuf/custom formats",
                "category": "Serialization",
                "origin": "Official",
            },
        ]
        with suppress(Exception):
            self.refresh_catalog()
