"""
PYTEST TESTING FRAMEWORK
Complete setup for comprehensive testing

Installation:
pip install pytest pytest-cov pytest-mock pytest-xdist pytest-asyncio
"""

# ══════════════════════════════════════════════════════════════════════
# File: conftest.py - Pytest Configuration and Fixtures
# ══════════════════════════════════════════════════════════════════════

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config import AppConfig, APIConfig, PathConfig, SearchConfig, DownloadConfig
from api_client import GoogleCustomSearchClient, SearchResult
from search_manager import SearchManager, DateRange
from query_manager import QueryManager, Query
from data_extractor import DataExtractor


# ══════════════════════════════════════════════════════════════════════
# FIXTURES - Reusable test objects
# ══════════════════════════════════════════════════════════════════════

@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def api_config():
    """Create test API configuration."""
    return APIConfig(
        search_engine_id="test_engine_id",
        api_keys=["test_key_1", "test_key_2", "test_key_3"],
        results_per_page=10,
        rate_limit_delay=0.1
    )


@pytest.fixture
def path_config(temp_dir):
    """Create test path configuration."""
    return PathConfig(
        base_path=temp_dir,
        results_subdir="_results"
    )


@pytest.fixture
def search_config():
    """Create test search configuration."""
    return SearchConfig(
        query="test query",
        exact_term="test",
        file_type="pdf",
        days_per_chunk=7
    )


@pytest.fixture
def download_config():
    """Create test download configuration."""
    return DownloadConfig(
        browser_headless=True,
        download_timeout=10,
        page_load_timeout=5
    )


@pytest.fixture
def app_config(api_config, path_config, search_config, download_config):
    """Create complete test application configuration."""
    return AppConfig(
        api=api_config,
        paths=path_config,
        search=search_config,
        download=download_config
    )


@pytest.fixture
def mock_search_result():
    """Create mock search result."""
    return SearchResult(
        title="Test Result",
        snippet="This is a test snippet",
        link="https://example.com/test",
        display_link="example.com",
        author="Test Author"
    )


@pytest.fixture
def query_manager():
    """Create query manager with test queries."""
    manager = QueryManager()
    manager.add_query(Query(
        name="test_query_1",
        query="test search 1",
        max_results=10
    ))
    manager.add_query(Query(
        name="test_query_2",
        query="test search 2",
        file_type="pdf",
        max_results=20
    ))
    return manager


@pytest.fixture
def data_extractor():
    """Create data extractor."""
    return DataExtractor()


@pytest.fixture
def mock_api_response():
    """Create mock API response."""
    return {
        'searchInformation': {
            'totalResults': '100',
            'searchTime': 0.5
        },
        'items': [
            {
                'title': 'Test Title 1',
                'snippet': 'Test snippet 1',
                'link': 'https://example.com/1',
                'displayLink': 'example.com',
                'pagemap': {
                    'metatags': [{'author': 'Author 1'}]
                }
            }
        ]
    }


# ══════════════════════════════════════════════════════════════════════
# MARKERS - Tag tests for selective running
# ══════════════════════════════════════════════════════════════════════

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow tests (require API calls)")
    config.addinivalue_line("markers", "api: Tests requiring real API calls")


# ══════════════════════════════════════════════════════════════════════
# File: pytest.ini - Pytest Configuration
# ══════════════════════════════════════════════════════════════════════

"""
[pytest]
minversion = 6.0
addopts = 
    -v
    --strict-markers
    --tb=short
    --cov=src
    --cov-report=html
    --cov-report=term-missing
    -m "not slow"
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
"""


# ══════════════════════════════════════════════════════════════════════
# File: test_config.py - Configuration Tests
# ══════════════════════════════════════════════════════════════════════

class TestAPIConfig:
    """Test APIConfig class."""
    
    def test_api_config_valid(self, api_config):
        """Test valid API configuration."""
        api_config.validate()
        assert api_config.search_engine_id == "test_engine_id"
        assert len(api_config.api_keys) == 3
    
    def test_api_config_empty_engine_id(self):
        """Test empty search engine ID raises error."""
        config = APIConfig(search_engine_id="", api_keys=["key"])
        with pytest.raises(ValueError):
            config.validate()
    
    def test_api_config_no_keys(self):
        """Test missing API keys raises error."""
        config = APIConfig(search_engine_id="test", api_keys=[])
        with pytest.raises(ValueError):
            config.validate()


class TestPathConfig:
    """Test PathConfig class."""
    
    def test_path_config_creates_dirs(self, path_config):
        """Test that directories are created."""
        assert path_config.base_path.exists()
        assert path_config.results_path.exists()
    
    def test_path_config_results_path(self, path_config):
        """Test results path construction."""
        expected = path_config.base_path / "_results"
        assert path_config.results_path == expected


class TestAppConfig:
    """Test AppConfig class."""
    
    def test_app_config_from_dict(self, app_config):
        """Test creating AppConfig from dictionary."""
        config_dict = app_config.to_dict()
        
        assert config_dict['api']['search_engine_id'] == "test_engine_id"
        assert len(config_dict['api']['api_keys']) == 3
        assert config_dict['search']['query'] == "test query"


# ══════════════════════════════════════════════════════════════════════
# File: test_query_manager.py - Query Manager Tests
# ══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestQueryManager:
    """Test QueryManager class."""
    
    def test_add_single_query(self, query_manager):
        """Test adding a single query."""
        initial_count = query_manager.get_count()
        query = Query(name="new_query", query="new search")
        query_manager.add_query(query)
        
        assert query_manager.get_count() == initial_count + 1
    
    def test_get_query_by_name(self, query_manager):
        """Test retrieving query by name."""
        query = query_manager.get_query_by_name("test_query_1")
        
        assert query is not None
        assert query.query == "test search 1"
    
    def test_get_nonexistent_query(self, query_manager):
        """Test getting non-existent query."""
        query = query_manager.get_query_by_name("nonexistent")
        
        assert query is None
    
    def test_export_to_csv(self, query_manager, temp_dir):
        """Test exporting queries to CSV."""
        output_file = temp_dir / "queries.csv"
        query_manager.export_to_csv(output_file)
        
        assert output_file.exists()
        
        # Verify content
        with open(output_file) as f:
            lines = f.readlines()
            assert len(lines) > 1  # Header + data
    
    def test_clear_queries(self, query_manager):
        """Test clearing all queries."""
        query_manager.clear()
        assert query_manager.get_count() == 0


# ══════════════════════════════════════════════════════════════════════
# File: test_data_extractor.py - Data Extractor Tests
# ══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestDataExtractor:
    """Test DataExtractor class."""
    
    def test_extract_txt_file(self, data_extractor, temp_dir):
        """Test extracting text from TXT file."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_content = "This is test content"
        test_file.write_text(test_content)
        
        result = data_extractor.extract(test_file)
        
        assert result.is_successful()
        assert result.file_type == "txt"
        assert test_content in result.content
    
    def test_extract_json_file(self, data_extractor, temp_dir):
        """Test extracting JSON file."""
        import json
        
        test_file = temp_dir / "test.json"
        test_data = {"key": "value", "number": 42}
        test_file.write_text(json.dumps(test_data))
        
        result = data_extractor.extract(test_file)
        
        assert result.is_successful()
        assert result.file_type == "json"
    
    def test_extract_csv_file(self, data_extractor, temp_dir):
        """Test extracting CSV file."""
        test_file = temp_dir / "test.csv"
        test_file.write_text("name,age\\nJohn,30\\nJane,25")
        
        result = data_extractor.extract(test_file)
        
        assert result.is_successful()
        assert result.file_type == "csv"
        assert result.metadata['rows'] > 0
    
    def test_extract_unsupported_format(self, data_extractor, temp_dir):
        """Test extracting unsupported file format."""
        test_file = temp_dir / "test.xyz"
        test_file.write_text("content")
        
        result = data_extractor.extract(test_file)
        
        assert not result.is_successful()
        assert "Unsupported" in result.error
    
    def test_save_extracted_data(self, data_extractor, temp_dir):
        """Test saving extracted data."""
        from data_extractor import ExtractedData
        
        data = ExtractedData(
            source_file=Path("test.txt"),
            file_type="txt",
            content="Test content",
            metadata={"test": True}
        )
        
        output_dir = temp_dir / "output"
        result_file = data_extractor.save_extracted_data(data, output_dir)
        
        assert result_file.exists()
        assert "extracted" in result_file.name


# ══════════════════════════════════════════════════════════════════════
# Running Tests - Command Reference
# ══════════════════════════════════════════════════════════════════════

"""
# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_config.py

# Run specific test class
pytest tests/test_config.py::TestAPIConfig

# Run specific test function
pytest tests/test_config.py::TestAPIConfig::test_api_config_valid

# Run tests matching a pattern
pytest -k "test_api"

# Run tests with a specific marker
pytest -m unit
pytest -m "not slow"

# Run tests in parallel (fast)
pytest -n auto

# Run with detailed output
pytest -v

# Run with print statements visible
pytest -s

# Run with short traceback
pytest --tb=short

# Stop after first failure
pytest -x

# Stop after N failures
pytest --maxfail=3

# Run tests in random order (catch interdependencies)
pytest --random-order
"""
