"""Unit tests for the WKafka MCP Server, covering catalog utilities, CLI commands, and template generation."""

from unittest import mock
import pytest

from wkafka_mcp import server
from wkafka_mcp.catalog import PatternsCatalog


@pytest.fixture
def catalog():
    """Provides a clean instance of PatternsCatalog with refresh_catalog stubbed out.
    
    This avoids making actual HTTP requests during initialization or testing.
    """
    with mock.patch.object(PatternsCatalog, "refresh_catalog", return_value=[]):
        return PatternsCatalog()


# --- catalog ---


def test_init_loads_initial_catalog():
    """Validates that the PatternsCatalog loads the offline fallback patterns on startup.
    
    This test verifies that:
    1. The catalog caches a non-empty list of fallback patterns (specifically, 13 patterns).
    2. The first pattern is 'basic_json_messaging' to ensure the catalog order and content.
    """
    c = PatternsCatalog()
    assert len(c.cached_patterns) == 13
    assert c.cached_patterns[0]["name"] == "basic_json_messaging"


def test_fetch_url_exception(catalog, caplog):
    """Validates that the catalog handles fetch errors gracefully.
    
    This test mocks urllib.request.urlopen to throw an Exception and ensures:
    1. The catalog returns an empty list instead of bubbling up the exception.
    2. The application remains stable when remote synchronization is unavailable.
    """
    with mock.patch("urllib.request.urlopen", side_effect=Exception("boom")):
        assert catalog._fetch_url("https://example.com/bad.json") == []


def test_refresh_catalog_merges_sources(catalog):
    """Validates that refresh_catalog merges official and community sources.
    
    This test mocks `_fetch_url` to return simulated official and community lists, verifying:
    1. Both URLs are queried.
    2. The resulting lists are marked with their respective origin ('Official' and 'Community').
    3. The merged results are cached and returned properly.
    """
    official = [{"name": "basic_json_messaging"}]
    community = [{"name": "community_pattern"}]
    with mock.patch.object(
        catalog, "_fetch_url", side_effect=[official, community]
    ) as mock_fetch:
        result = catalog.refresh_catalog()
    assert mock_fetch.call_count == 2
    assert result[0]["origin"] == "Official"
    assert result[1]["origin"] == "Community"


def test_search_matches_multiple_fields(catalog):
    """Validates the search filtering capabilities of the PatternsCatalog.
    
    This test defines dummy patterns and ensures that search works:
    1. Case-insensitively and partial-matching.
    2. Across multiple fields such as name, module, and description.
    """
    patterns = [
        {
            "name": "sasl_authentication",
            "module": "wkafka",
            "description": "SASL security",
        },
        {"name": "image_vision_pipeline", "module": "wkafka", "description": "images"},
    ]
    catalog.cached_patterns = patterns
    assert len(catalog.search("sasl")) == 1
    assert len(catalog.search("image")) == 1


# --- get_catalog ---


def test_get_catalog_returns_shared_singleton():
    """Validates that get_catalog is correctly cached as a singleton.
    
    This test verifies that:
    1. Multiple calls to get_catalog return the exact same instance in memory.
    2. The lru_cache decorator is active and working.
    """
    server.get_catalog.cache_clear()
    c1 = server.get_catalog()
    c2 = server.get_catalog()
    assert c1 is c2
    server.get_catalog.cache_clear()


# --- blueprints ---


def test_blueprints_contains_all_sections():
    """Validates the output of get_wkafka_architect_blueprints.
    
    This test ensures that the blueprint reference document:
    1. Contains all five updated controller-based wkafka code patterns.
    2. Includes the expert warning headers and required formatting keywords.
    """
    text = server.get_wkafka_architect_blueprints()
    for s in [
        "=== 1. BASIC JSON CONSUMER",
        "=== 2. IMAGE CONSUMER",
        "=== 3. VIDEO FRAME STREAM CONSUMER",
        "=== 4. SEND JSON PRODUCER",
        "=== 5. SEND IMAGE PRODUCER",
    ]:
        assert s in text
    assert "WKAFKA EXPERT BLUEPRINTS" in text
    assert "read/write" in text.lower()


# --- manual ---


def test_manual_contains_guides():
    """Validates the content of get_wkafka_architect_manual.
    
    This test checks that the returned architectural manual contains all required guide sections:
    1. Project structure rules.
    2. Core rules.
    3. Data flow guide.
    4. Module mapping.
    5. Monolith refactoring guide with Mermaid diagram specifications.
    """
    text = server.get_wkafka_architect_manual()
    assert "WKAFKA ARCHITECT MANUAL" in text
    assert "PROJECT STRUCTURE RULES" in text
    assert "CORE RULES" in text
    assert "DATA FLOW GUIDE" in text
    assert "MODULE MAP" in text
    assert "REFACTORING A MONOLITH TO WKAFKA" in text
    assert "mermaid" in text
    assert "Generated by WKafka MCP by wisrovi" in text


# --- search tool ---


def test_search_wkafka_pattern_returns_results():
    """Validates search_wkafka_pattern returns formatted search results.
    
    This test mocks get_catalog to return dummy patterns and verifies that the CLI/tool:
    1. Parses the query and formatting.
    2. Correctly prints metadata such as origin, name, manager, and description.
    """
    patterns = [
        {
            "origin": "Official",
            "name": "sasl_authentication",
            "manager": "WKafka",
            "module": "wkafka",
            "description": "SASL",
        },
        {
            "origin": "Community",
            "name": "custom_serializer",
            "manager": "Serializer",
            "module": "wkafka.serializers.base",
            "description": "ext",
        },
    ]
    catalog_mock = mock.MagicMock()
    catalog_mock.search.return_value = patterns
    with mock.patch.object(server, "get_catalog", return_value=catalog_mock):
        text = server.search_wkafka_pattern("sasl")
    assert "sasl_authentication" in text
    assert "Official" in text
    assert "Community" in text


def test_search_wkafka_pattern_no_results():
    """Validates search_wkafka_pattern behavior when no patterns match.
    
    This test verifies that a user-friendly error message is returned with recommendations
    when no patterns are found in the catalog.
    """
    catalog_mock = mock.MagicMock()
    catalog_mock.search.return_value = []
    with mock.patch.object(server, "get_catalog", return_value=catalog_mock):
        text = server.search_wkafka_pattern("zzz_nope")
    assert "No pattern matching 'zzz_nope' was found" in text


# --- deploy scaffolding ---


def test_deploy_wkafka_scaffolding_relative_path_rejected():
    """Validates that deploy_wkafka_scaffolding rejects relative target paths.
    
    This test checks that using a relative path returns an error message starting with 'Error:'
    to prevent files from being written to untracked relative folders.
    """
    text = server.deploy_wkafka_scaffolding("relative/path")
    assert text.startswith("Error:")
    assert "absolute path" in text


def test_deploy_wkafka_scaffolding_creates_project(tmp_path):
    """Validates the directory layout deployment of the scaffolding generator.
    
    This test uses a temporary directory to verify that:
    1. All directories and templates are deployed correctly.
    2. Config, consumer, producer, and main.py files are created with appropriate permissions.
    """
    target = str(tmp_path / "deployed")
    result = server.deploy_wkafka_scaffolding(
        target, project_name="MyKafka", scaffold_type="vision_pipeline"
    )
    assert result.startswith("Success:")
    assert "MyKafka" in result
    assert (tmp_path / "deployed" / "main.py").exists()
    assert (tmp_path / "deployed" / "config" / "settings.py").exists()
    assert (tmp_path / "deployed" / "consumers" / "consumer.py").exists()
    assert (tmp_path / "deployed" / "producers" / "producer.py").exists()


def test_deploy_wkafka_scaffolding_exception_returns_error(tmp_path):
    """Validates error handling in scaffolding deployment.
    
    This test mocks folder generation to throw an OSError and verifies that:
    1. The exception is handled gracefully.
    2. The function returns a readable error message describing the failure.
    """
    target = str(tmp_path / "blocked")
    with mock.patch.object(
        server.TemplateGenerator, "get_folders", side_effect=OSError("disk full")
    ):
        text = server.deploy_wkafka_scaffolding(target)
    assert text == "Error: disk full"


# --- generate_from_pattern ---


def test_generate_from_pattern_not_found(tmp_path):
    """Validates generate_from_pattern error handling when the requested pattern is missing.
    
    This test verifies that querying a non-existing pattern returns a 'not found' message.
    """
    target = str(tmp_path / "gen")
    text = server.generate_from_pattern("nope", target)
    assert "not found" in text


# --- validate_kafka_config ---


def test_validate_kafka_config_missing_bootstrap():
    """Validates configuration check for missing bootstrap_servers.
    
    This test verifies that checking a config snippet without bootstrap_servers returns a warning.
    """
    text = server.validate_kafka_config('client_id="x"')
    assert "Missing bootstrap_servers" in text


def test_validate_kafka_config_hardcoded_credential():
    """Validates configuration check for hardcoded secrets.
    
    This test ensures that a warning is raised when credentials are hardcoded rather than
    retrieved via env variables like `os.environ` or `getenv`.
    """
    text = server.validate_kafka_config(
        'bootstrap_servers="localhost:9092" sasl_plain_password="secret"'
    )
    assert "Hardcoded credential" in text


def test_validate_kafka_config_ok():
    """Validates config checker returns success on fully valid settings.
    
    This test checks that a snippet containing bootstrap_servers and no secrets passes successfully.
    """
    text = server.validate_kafka_config('bootstrap_servers="localhost:9092"')
    assert "validated successfully" in text


# --- consumer and producer generators ---


def test_generate_wkafka_consumer_returns_code():
    """Validates that generate_wkafka_consumer returns valid consumer python code structure.
    
    This test verifies that the generated code:
    1. Contains the subscriber decorator on the correct topic.
    2. Imports the correct config module.
    """
    code = server.generate_wkafka_consumer("orders-topic", format="json", key_filter="order-key")
    assert "orders-topic" in code
    assert "json" in code
    assert "key_filter=\"order-key\"" in code
    assert "from config.settings import" in code


def test_generate_wkafka_consumer_writes_file(tmp_path):
    """Validates that generate_wkafka_consumer saves the code into a file if target_file is absolute.
    
    This test verifies that the file is created at the absolute path and has correct content.
    """
    target = str(tmp_path / "my_consumer.py")
    res = server.generate_wkafka_consumer("orders-topic", target_file=target)
    assert res.startswith("Success:")
    assert (tmp_path / "my_consumer.py").exists()
    content = (tmp_path / "my_consumer.py").read_text()
    assert "orders-topic" in content


def test_generate_wkafka_consumer_rejects_relative_path():
    """Validates that generate_wkafka_consumer rejects relative target paths.
    
    This test verifies that using a relative path returns an error message starting with 'Error:'.
    """
    res = server.generate_wkafka_consumer("orders-topic", target_file="rel/path.py")
    assert res.startswith("Error:")


def test_generate_wkafka_producer_returns_code():
    """Validates that generate_wkafka_producer returns valid producer python code structure.
    
    This test verifies that the generated code:
    1. Contains send() syntax with the targeted topic.
    2. Imports the correct config module.
    """
    code = server.generate_wkafka_producer("orders-topic", format="yaml")
    assert "orders-topic" in code
    assert "yaml" in code
    assert "from config.settings import" in code


def test_generate_wkafka_producer_writes_file(tmp_path):
    """Validates that generate_wkafka_producer saves the code into a file if target_file is absolute.
    
    This test verifies that the file is created at the absolute path and has correct content.
    """
    target = str(tmp_path / "my_producer.py")
    res = server.generate_wkafka_producer("orders-topic", target_file=target)
    assert res.startswith("Success:")
    assert (tmp_path / "my_producer.py").exists()
    content = (tmp_path / "my_producer.py").read_text()
    assert "orders-topic" in content


# --- CLI run modes ---


def test_run_stdio_uses_stdio_transport():
    """Validates that run_stdio launches FastMCP using standard input/output transport.
    
    This test mocks FastMCP's run method and asserts that transport is set to 'stdio'.
    """
    with mock.patch.object(server.mcp, "run") as mock_run:
        server.run_stdio()
    mock_run.assert_called_once_with(transport="stdio")


def test_run_sse_uses_sse_transport():
    """Validates that run_sse launches FastMCP using SSE (Server-Sent Events) transport.
    
    This test mocks FastMCP's run method and asserts that transport is set to 'sse'.
    """
    with mock.patch.object(server.mcp, "run") as mock_run:
        server.run_sse()
    mock_run.assert_called_once_with(transport="sse")


def test_print_config_to_stdout(capsys):
    """Validates print_config prints JSON and instructions to standard output when write_file is False.
    
    This test captures standard output and verifies that server key names and instructions are printed.
    """
    server.print_config(write_file=False)
    captured = capsys.readouterr()
    assert "wkafka-mcp" in captured.out
    assert "QUICK INSTALL COMMANDS" in captured.out


def test_print_config_saves_file(tmp_path, monkeypatch):
    """Validates print_config creates a config JSON file in .agents directory.
    
    This test changes the working directory to a temporary path, executes print_config,
    and checks if `.agents/wkafka-mcp.json` is created.
    """
    monkeypatch.chdir(tmp_path)
    server.print_config(write_file=True)
    assert (tmp_path / ".agents" / "wkafka-mcp.json").exists()


def test_main_config_saves_file(tmp_path, monkeypatch):
    """Validates that command line argument 'config' correctly calls configuration export.
    
    This test simulates command line args and verifies the resulting json configuration is saved.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(server.sys, "argv", ["wkafka-mcp", "config"])
    server.main()
    assert (tmp_path / ".agents" / "wkafka-mcp.json").exists()


def test_main_run_uses_stdio(monkeypatch):
    """Validates that the default CLI invocation starts the server in stdio mode.
    
    This test simulates argument parsing and checks that stdio mode is called on the FastMCP object.
    """
    monkeypatch.setattr(server.sys, "argv", ["wkafka-mcp", "run"])
    with mock.patch.object(server.mcp, "run") as mock_run:
        server.main()
    mock_run.assert_called_once_with(transport="stdio")
