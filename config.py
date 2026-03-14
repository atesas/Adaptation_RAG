"""Configuration for Climate Risk Extraction System"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Azure OpenAI
    AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
    AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
    AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4-turbo")
    AZURE_EMBEDDING_DEPLOYMENT = os.getenv("AZURE_EMBEDDING_DEPLOYMENT", "text-embedding-3-large")
    OPENAI_API_VERSION = "2024-10-01-preview"

    # Azure AI Search
    AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
    AZURE_SEARCH_KEY = os.getenv("AZURE_SEARCH_KEY")
    AZURE_SEARCH_TEMP_INDEX = os.getenv("AZURE_SEARCH_TEMP_INDEX", "climate-temp-index")

    # Processing
    CHUNK_SIZE = 1000
    CHUNK_OVERLAP = 200
    MAX_SEARCH_RESULTS = 7
    EMBEDDING_DIMENSIONS = 3072
    EMBEDDING_BATCH_SIZE = 20
    UPLOAD_BATCH_SIZE = 100

    # Folders
    OUTPUT_FOLDER = "output"
    RAW_FOLDER = "output/raw"
    TABLES_FOLDER = "output/tables"
    LOGS_FOLDER = "output/logs"
    MASTER_FOLDER = "output/tables/master"
    RISK_ID_FOLDER = "output/tables/risk_identification"
    FINANCIAL_FOLDER = "output/tables/financial"
    RESPONSES_FOLDER = "output/tables/responses"
    MANAGEMENT_FOLDER = "output/tables/management"
    METADATA_FOLDER = "output/tables/metadata"

    @classmethod
    def validate(cls):
        """Validate required environment variables"""
        required = [
            "AZURE_OPENAI_KEY",
            "AZURE_OPENAI_ENDPOINT",
            "AZURE_SEARCH_ENDPOINT",
            "AZURE_SEARCH_KEY"
        ]
        missing = [var for var in required if not getattr(cls, var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        return True

    @classmethod
    def create_folders(cls):
        """Create all required output folders"""
        folders = [
            cls.OUTPUT_FOLDER,
            cls.RAW_FOLDER,
            cls.TABLES_FOLDER,
            cls.LOGS_FOLDER,
            cls.MASTER_FOLDER,
            cls.RISK_ID_FOLDER,
            cls.FINANCIAL_FOLDER,
            cls.RESPONSES_FOLDER,
            cls.MANAGEMENT_FOLDER,
            cls.METADATA_FOLDER
        ]

        for folder in folders:
            Path(folder).mkdir(parents=True, exist_ok=True)
