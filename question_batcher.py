"""
Question batching configuration for efficient extraction
"""

# Question batch groups - optimized for single LLM calls
QUESTION_BATCHES = {
    "batch_1_metadata": {
        "name": "Document Metadata & Frameworks",
        "questions": ["Q0", "Q1", "Q2"],
        "description": "Gate check, company info, framework references"
    },

    "batch_2_scope": {
        "name": "Assessment Scope & Methodology",
        "questions": ["Q3", "Q5", "Q10"],
        "description": "Coverage, scenarios, materiality methodology"
    },

    "batch_3_hazards": {
        "name": "Physical Risk Identification",
        "questions": ["Q4", "Q12"],
        "description": "Specific hazards and location-specific risks"
    },

    "batch_4_vulnerability": {
        "name": "Vulnerability Assessment",
        "questions": ["Q6", "Q7", "Q8", "Q9"],
        "description": "Assets, supply chain, comprehensive risks, vulnerabilities"
    },

    "batch_5_financial": {
        "name": "Financial Quantification",
        "questions": ["Q11", "Q18", "Q23"],
        "description": "Cost estimates, CapEx/OpEx, scenario-based impacts"
    },

    "batch_6_adaptation": {
        "name": "Adaptation Measures",
        "questions": ["Q13", "Q14", "Q15", "Q16", "Q17", "Q22"],
        "description": "Risk-adaptation linkage, measures, programs, nature-based solutions, innovation, KPIs"
    },

    "batch_7_governance": {
        "name": "Governance & Integration",
        "questions": ["Q19", "Q20", "Q21"],
        "description": "Governance structure, business integration, stakeholder engagement"
    }
}

def get_questions_for_batch(batch_id, all_questions):
    """
    Get question texts for a specific batch

    Args:
        batch_id: Batch identifier (e.g., 'batch_1_metadata')
        all_questions: Dict of {question_id: question_text}

    Returns:
        List of question dicts with id and text
    """
    batch_config = QUESTION_BATCHES.get(batch_id)
    if not batch_config:
        return []

    question_ids = batch_config["questions"]
    return [
        {"id": qid, "text": all_questions.get(qid, "")}
        for qid in question_ids
        if qid in all_questions
    ]

def should_skip_remaining_batches(batch_1_results):
    """
    Check if Q0 indicates document is not relevant

    Args:
        batch_1_results: Results from batch_1_metadata

    Returns:
        bool: True if should skip further extraction
    """
    if "Q0" not in batch_1_results:
        return False

    q0_data = batch_1_results["Q0"]

    # Check various ways LLM might indicate irrelevance
    is_relevant = q0_data.get("is_relevant", True)
    skip_extraction = q0_data.get("skip_further_extraction", False)

    # Handle string boolean values
    if isinstance(is_relevant, str):
        is_relevant = is_relevant.lower() in ["true", "yes", "1"]

    if isinstance(skip_extraction, str):
        skip_extraction = skip_extraction.lower() in ["true", "yes", "1"]

    return (not is_relevant) or skip_extraction
