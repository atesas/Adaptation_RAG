"""
Query Manager - Handle multiple queries from list or file

Supports:
- Query lists (CSV, JSON, YAML)
- Batch processing with scheduling
- Query templates with variables
- Result aggregation
"""

import csv
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
import yaml


logger = logging.getLogger(__name__)


@dataclass
class Query:
    """Represents a single search query."""
    name: str
    query: str
    exact_term: str = ""
    file_type: str = ""
    language: str = ""
    country: str = ""
    start_date: str = ""
    end_date: str = ""
    days_per_chunk: int = 7
    max_results: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'query': self.query,
            'exact_term': self.exact_term,
            'file_type': self.file_type,
            'language': self.language,
            'country': self.country,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'days_per_chunk': self.days_per_chunk,
            'max_results': self.max_results
        }


class QueryManager:
    """Manages multiple queries from various sources."""
    
    def __init__(self):
        self.queries: List[Query] = []
        logger.info("QueryManager initialized")
    
    def add_query(self, query: Query) -> None:
        """Add a single query."""
        self.queries.append(query)
        logger.info(f"Added query: {query.name}")
    
    def add_queries(self, queries: List[Query]) -> None:
        """Add multiple queries at once."""
        self.queries.extend(queries)
        logger.info(f"Added {len(queries)} queries")
    
    def load_from_csv(self, csv_file: Path) -> None:
        """
        Load queries from CSV file.
        
        CSV format:
        name,query,exact_term,file_type,language,country,start_date,end_date,max_results
        climate_2024,climate adaptation,,,,,01/01/2024,31/12/2024,50
        transition_fund,transition fund investment,,,,,01/01/2023,31/12/2023,100
        """
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                query = Query(
                    name=row.get('name', '').strip(),
                    query=row.get('query', '').strip(),
                    exact_term=row.get('exact_term', '').strip(),
                    file_type=row.get('file_type', '').strip(),
                    language=row.get('language', '').strip(),
                    country=row.get('country', '').strip(),
                    start_date=row.get('start_date', '').strip(),
                    end_date=row.get('end_date', '').strip(),
                    days_per_chunk=int(row.get('days_per_chunk', 7)),
                    max_results=int(row.get('max_results')) if row.get('max_results') else None
                )
                self.add_query(query)
        
        logger.info(f"Loaded {len(self.queries)} queries from CSV")
    
    def load_from_json(self, json_file: Path) -> None:
        """
        Load queries from JSON file.
        
        JSON format:
        {
          "queries": [
            {
              "name": "climate_2024",
              "query": "climate adaptation",
              "start_date": "01/01/2024",
              "end_date": "31/12/2024",
              "max_results": 50
            }
          ]
        }
        """
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        for q_data in data.get('queries', []):
            query = Query(**q_data)
            self.add_query(query)
        
        logger.info(f"Loaded {len(self.queries)} queries from JSON")
    
    def load_from_yaml(self, yaml_file: Path) -> None:
        """
        Load queries from YAML file.
        
        YAML format:
        queries:
          - name: climate_2024
            query: climate adaptation
            start_date: 01/01/2024
            end_date: 31/12/2024
            max_results: 50
        """
        with open(yaml_file, 'r') as f:
            data = yaml.safe_load(f)
        
        for q_data in data.get('queries', []):
            query = Query(**q_data)
            self.add_query(query)
        
        logger.info(f"Loaded {len(self.queries)} queries from YAML")
    
    def load_from_file(self, file_path: Path) -> None:
        """Auto-detect format and load queries."""
        file_path = Path(file_path)
        
        if file_path.suffix == '.csv':
            self.load_from_csv(file_path)
        elif file_path.suffix == '.json':
            self.load_from_json(file_path)
        elif file_path.suffix in ['.yaml', '.yml']:
            self.load_from_yaml(file_path)
        else:
            raise ValueError(f"Unsupported format: {file_path.suffix}")
    
    def get_queries(self) -> List[Query]:
        """Get all queries."""
        return self.queries
    
    def get_query_by_name(self, name: str) -> Optional[Query]:
        """Get query by name."""
        for q in self.queries:
            if q.name == name:
                return q
        return None
    
    def export_to_csv(self, output_file: Path) -> None:
        """Export queries to CSV."""
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = [
                'name', 'query', 'exact_term', 'file_type', 'language',
                'country', 'start_date', 'end_date', 'days_per_chunk', 'max_results'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for query in self.queries:
                writer.writerow(query.to_dict())
        
        logger.info(f"Exported {len(self.queries)} queries to CSV")
    
    def clear(self) -> None:
        """Clear all queries."""
        self.queries.clear()
        logger.info("Cleared all queries")
    
    def get_count(self) -> int:
        """Get number of queries."""
        return len(self.queries)


# Example usage
if __name__ == '__main__':
    # Create query manager
    manager = QueryManager()
    
    # Add queries programmatically
    manager.add_query(Query(
        name="climate_adaptation",
        query="climate adaptation vulnerability",
        start_date="01/01/2024",
        end_date="31/12/2024",
        max_results=50
    ))
    
    manager.add_query(Query(
        name="transition_fund",
        query="transition fund investment",
        file_type="pdf",
        max_results=100
    ))
    
    # Export to CSV
    manager.export_to_csv(Path("queries.csv"))
    
    # Load from CSV
    manager.clear()
    manager.load_from_csv(Path("queries.csv"))
    
    # Print all queries
    for query in manager.get_queries():
        print(f"Query: {query.name} - {query.query}")
