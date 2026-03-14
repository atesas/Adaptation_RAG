"""
Generate unique document IDs
"""
import hashlib
import re
from pathlib import Path

def slugify(text):
    """Convert text to URL-friendly slug"""
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '_', text)
    text = text.strip('_')
    return text[:30]  # Limit length

def generate_document_id(file_path, company_name=None, year=None, doc_type=None):
    """
    Generate unique document ID: {company}_{year}_{type}_{hash4}

    Args:
        file_path: Path to the PDF file
        company_name: Company name (optional, extracted if None)
        year: Publication year (optional)
        doc_type: Document type (optional)

    Returns:
        document_id: Unique identifier string
    """
    filename = Path(file_path).name

    # Generate hash from filename
    hash_full = hashlib.md5(filename.encode()).hexdigest()
    hash4 = hash_full[:4]

    # Build ID components
    parts = []

    if company_name:
        parts.append(slugify(company_name))
    else:
        parts.append("unknown")

    if year:
        parts.append(str(year))
    else:
        parts.append("0000")

    if doc_type:
        # Shorten common document types
        doc_type_map = {
            "sustainability report": "sust",
            "annual report": "annual",
            "esg report": "esg",
            "climate report": "climate",
            "tcfd disclosure": "tcfd",
            "cdp response": "cdp",
            "integrated report": "integrated"
        }
        doc_slug = doc_type_map.get(doc_type.lower(), slugify(doc_type)[:10])
        parts.append(doc_slug)
    else:
        parts.append("report")

    parts.append(hash4)

    return "_".join(parts)

def update_document_id_after_extraction(temp_id, extracted_data):
    """
    Update document ID with extracted metadata

    Args:
        temp_id: Temporary ID (with unknown company)
        extracted_data: Dict with company_name, publication_year, document_type

    Returns:
        final_id: Updated document ID
    """
    # Extract hash from temp_id
    hash4 = temp_id.split('_')[-1]

    company = extracted_data.get('company_name', 'unknown')
    year = extracted_data.get('publication_year', '0000')
    doc_type = extracted_data.get('document_type', 'report')

    # Rebuild ID
    parts = [
        slugify(company),
        str(year),
        slugify(doc_type)[:10],
        hash4
    ]

    return "_".join(parts)
