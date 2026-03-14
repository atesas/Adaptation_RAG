"""
Load questions from text file
"""

def load_questions_from_file(file_path):
    """
    Load questions from text file

    Expected format:
        Q0: Question text... Output: {...}
        Q1: Question text... Output: {...}

    Returns:
        Dict of {question_id: question_text}
    """
    questions = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by question IDs (Q0, Q1, Q2, etc.)
    import re
    pattern = r'(Q\d+):\s*'
    parts = re.split(pattern, content)

    # Parse alternating IDs and texts
    for i in range(1, len(parts), 2):
        if i+1 < len(parts):
            question_id = parts[i]
            question_text = parts[i+1].strip()
            questions[question_id] = question_text

    return questions

def parse_output_schema(question_text):
    """
    Extract expected output schema from question text

    Args:
        question_text: Full question including "Output: {...}"

    Returns:
        Dict with schema structure or None
    """
    import re
    import json

    # Find Output: {...} pattern
    match = re.search(r'Output:\s*(\{[^}]+\})', question_text)
    if not match:
        return None

    schema_str = match.group(1)

    try:
        # Convert to valid JSON (replace single quotes, etc.)
        schema_str = schema_str.replace("'", '"')
        schema = json.loads(schema_str)
        return schema
    except:
        return None
