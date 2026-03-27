"""
Configuration management for Google Custom Search tool.
Supports both YAML and JSON configuration files.
"""
import os
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SearchConfig:
    """Configuration for search parameters."""
    query: str
    exact_term: str = ""
    file_type: str = ""
    language: str = ""
    country: str = ""
    start_date: str = ""
    end_date: str = ""
    days_per_chunk: int = 5
    
    def validate(self) -> None:
        """Validate search configuration."""
        if not self.query:
            raise ValueError("Query cannot be empty")
        if self.days_per_chunk <= 0:
            raise ValueError("days_per_chunk must be positive")


@dataclass
class APIConfig:
    """Configuration for Google Custom Search API."""
    search_engine_id: str
    api_keys: List[str]
    base_url: str = "https://www.googleapis.com/customsearch/v1"
    results_per_page: int = 10
    rate_limit_delay: float = 1.0
    
    def validate(self) -> None:
        """Validate API configuration."""
        if not self.search_engine_id:
            raise ValueError("search_engine_id cannot be empty")
        if not self.api_keys:
            raise ValueError("At least one API key is required")
        if self.results_per_page not in [1, 10]:
            raise ValueError("results_per_page must be 1 or 10")


@dataclass
class PathConfig:
    """Configuration for file paths."""
    base_path: Path
    results_subdir: str = "_results"
    api_keys_file: Optional[Path] = None
    
    def __post_init__(self):
        """Convert strings to Path objects and create directories."""
        if isinstance(self.base_path, str):
            self.base_path = Path(self.base_path)
        if isinstance(self.api_keys_file, str):
            self.api_keys_file = Path(self.api_keys_file)
            
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.results_path.mkdir(parents=True, exist_ok=True)
    
    @property
    def results_path(self) -> Path:
        """Get the full results directory path."""
        return self.base_path / self.results_subdir


@dataclass
class DownloadConfig:
    """Configuration for file downloads."""
    browser_headless: bool = True
    download_timeout: int = 30
    page_load_timeout: int = 10
    max_retries: int = 3


@dataclass
class AppConfig:
    """Main application configuration."""
    api: APIConfig
    paths: PathConfig
    search: Optional[SearchConfig] = None
    download: DownloadConfig = field(default_factory=DownloadConfig)
    
    @classmethod
    def from_file(cls, config_path: str) -> 'AppConfig':
        """Load configuration from YAML or JSON file."""
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            if config_path.suffix in ['.yaml', '.yml']:
                try:
                    import yaml
                    data = yaml.safe_load(f)
                except ImportError:
                    raise ImportError("pyyaml not installed. Run: pip install pyyaml")
            elif config_path.suffix == '.json':
                data = json.load(f)
            else:
                raise ValueError(f"Unsupported config file format: {config_path.suffix}")
        
        return cls.from_dict(data)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AppConfig':
        """Create configuration from dictionary."""
        api_config = APIConfig(**data['api'])
        path_config = PathConfig(**data['paths'])
        
        search_config = None
        if 'search' in data and data['search']:
            search_config = SearchConfig(**data['search'])
        
        download_config = DownloadConfig(**data.get('download', {}))
        
        config = cls(
            api=api_config,
            paths=path_config,
            search=search_config,
            download=download_config
        )
        
        config.validate()
        return config
    
    def validate(self) -> None:
        """Validate all configuration sections."""
        self.api.validate()
        if self.search:
            self.search.validate()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'api': {
                'search_engine_id': self.api.search_engine_id,
                'api_keys': self.api.api_keys,
                'base_url': self.api.base_url,
                'results_per_page': self.api.results_per_page,
                'rate_limit_delay': self.api.rate_limit_delay
            },
            'paths': {
                'base_path': str(self.paths.base_path),
                'results_subdir': self.paths.results_subdir,
                'api_keys_file': str(self.paths.api_keys_file) if self.paths.api_keys_file else None
            },
            'search': {
                'query': self.search.query,
                'exact_term': self.search.exact_term,
                'file_type': self.search.file_type,
                'language': self.search.language,
                'country': self.search.country,
                'start_date': self.search.start_date,
                'end_date': self.search.end_date,
                'days_per_chunk': self.search.days_per_chunk
            } if self.search else None,
            'download': {
                'browser_headless': self.download.browser_headless,
                'download_timeout': self.download.download_timeout,
                'page_load_timeout': self.download.page_load_timeout,
                'max_retries': self.download.max_retries
            }
        }
