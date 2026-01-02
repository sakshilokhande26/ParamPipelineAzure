import logging
import azure.functions as func
import pandas as pd
import json
import re
import io
from datetime import datetime

app = func.FunctionApp()

# ============================================================
# CHARACTERS THAT CAN BE CLEANED
# ============================================================
CLEANABLE_CHARS = {
    '™': '',           # Trademark
    '®': '',           # Registered
    '©': '',           # Copyright
    '"': '"',          # Smart quote left
    '"': '"',          # Smart quote right
    ''': "'",          # Smart apostrophe left
    ''': "'",          # Smart apostrophe right
    '–': '-',          # En dash
    '—': '-',          # Em dash
    '…': '...',        # Ellipsis
    '\u200B': '',      # Zero-width space
    '\uFEFF': '',      # BOM
    '_': ' ',          # Underscore to space (in names)
}

# ============================================================
# CHARACTERS THAT CANNOT BE CLEANED (Must be reported)
# ============================================================
UNCLENABLE_PATTERNS = {
    'null_char': {
        'pattern': r'\x00',
        'description': 'NULL character - Binary data corruption',
        'example': '\\x00'
    },
    'control_chars': {
        'pattern': r'[\x01-\x08\x0B\x0C\x0E-\x1F]',
        'description': 'Control characters - Mainframe/Legacy system artifacts',
        'example': '\\x01-\\x1F'
    },
    'escape_char': {
        'pattern': r'\x1B',
        'description': 'Escape character - Terminal sequences',
        'example': '\\x1B'
    },
    'ebcdic_newline': {
        'pattern': r'\x85',
        'description': 'EBCDIC Newline - Mainframe data',
        'example': '\\x85'
    },
    'ebcdic_artifacts': {
        'pattern': r'[\x8D\x8F\x90\x9D]',
        'description': 'EBCDIC conversion artifacts - Mainframe migration issue',
        'example': '\\x8D, \\x8F, etc.'
    },
    'replacement_char': {
        'pattern': r'\uFFFD',
        'description': 'Replacement character - Encoding error (data loss)',
        'example': '�'
    },
    'private_use': {
        'pattern': r'[\uE000-\uF8FF]',
        'description': 'Private Use Area - Custom/proprietary characters',
        'example': 'Private Unicode'
    }
}


def clean_text(text):
    """
    Clean text by replacing cleanable characters.
    Returns cleaned text.
    """
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text)
    
    # Replace all cleanable characters
    for old_char, new_char in CLEANABLE_CHARS.items():
        text = text.replace(old_char, new_char)
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    return text.strip()


def detect_unclenable_chars(text):
    """
    Detect characters that CANNOT be cleaned.
    Returns list of issues found.
    """
    if pd.isna(text) or text is None:
        return []
    
    text = str(text)
    issues = []
    
    for issue_type, config in UNCLENABLE_PATTERNS.items():
        matches = re.findall(config['pattern'], text)
        if matches:
            issues.append({
                'type': issue_type,
                'description': config['description'],
                'example': config['example'],
                'count': len(matches),
                'found_chars': [repr(m) for m in matches[:3]]
            })
    
    return issues


def has_unclenable_chars(text):
    """Check if text has any unclenable characters."""
    return len(detect_unclenable_chars(text)) > 0


def process_dataframe(df, file_name):
    """
    Process dataframe:
    1. Identify rows with unclenable characters
    2. Clean the rows that CAN be cleaned
    3. Return clean rows, dirty rows, and column analysis
    """
    
    # Track which rows have unclenable issues
    dirty_row_indices = []
    dirty_row_details = []
    
    # Check each row for unclenable characters
    for idx, row in df.iterrows():
        row_issues = []
        
        for col in df.columns:
            value = row[col]
            if pd.notna(value) and isinstance(value, str):
                issues = detect_unclenable_chars(value)
                if issues:
                    row_issues.append({
                        'column': col,
                        'value': str(value)[:100],  # Truncate for display
                        'issues': issues
                    })
        
        if row_issues:
            dirty_row_indices.append(idx)
            dirty_row_details.append({
                'row_number': int(idx) + 2,  # +2 for header and 0-index
                'row_data': {k: str(v)[:50] for k, v in row.to_dict().items()},
                'problems': row_issues
            })
    
    # Separate clean and dirty rows
    clean_df = df.drop(dirty_row_indices).copy()
    dirty_df = df.loc[dirty_row_indices].copy() if dirty_row_indices else pd.DataFrame()
    
    # Clean the clean rows (fix cleanable characters)
    for col in clean_df.columns:
        if clean_df[col].dtype == 'object':
            clean_df[col] = clean_df[col].apply(clean_text)
    
    # Analyze which columns had issues
    column_analysis = {}
    for col in df.columns:
        if df[col].dtype == 'object':
            has_issues = any(
                has_unclenable_chars(str(v)) 
                for v in df[col].dropna()
            )
            column_analysis[col] = {
                'type': 'string',
                'had_unclenable_chars': has_issues
            }
    
    return clean_df, dirty_df, dirty_row_details, column_analysis


@app.route(route="CleanDataFunction", auth_level=func.AuthLevel.ANONYMOUS)
def CleanDataFunction(req: func.HttpRequest) -> func.HttpResponse:
    """
    Main function to clean data.
    
    Input JSON:
    {
        "fileName": "customers.csv",
        "folderPath": "",
        "fileContent": "base64 encoded content",
        "targetTable": "Customers"
    }
    
    Output JSON:
    {
        "status": "SUCCESS",
        "originalRowCount": 100,
        "cleanRowCount": 95,
        "dirtyRowCount": 5,
        "hasDirtyRows": true,
        "cleanData": "base64 encoded clean CSV",
        "dirtyRowDetails": [...],
        "columnAnalysis": {...}
    }
    """
    logging.info('CleanDataFunction triggered')
    
    try:
        req_body = req.get_json()
        
        file_name = req_body.get('fileName', 'unknown.csv')
        folder_path = req_body.get('folderPath', '')
        file_content_b64 = req_body.get('fileContent', '')
        target_table = req_body.get('targetTable', 'DefaultTable')
        
        # Decode file content
        import base64
        file_content = base64.b64decode(file_content_b64)
        
        # Read CSV
        df = pd.read_csv(io.BytesIO(file_content))
        original_row_count = len(df)
        
        logging.info(f"Processing {file_name}: {original_row_count} rows")
        
        # Process the dataframe
        clean_df, dirty_df, dirty_row_details, column_analysis = process_dataframe(df, file_name)
        
        clean_row_count = len(clean_df)
        dirty_row_count = len(dirty_df)
        
        logging.info(f"Clean: {clean_row_count}, Dirty: {dirty_row_count}")
        
        # Convert clean data to base64 CSV
        clean_csv = clean_df.to_csv(index=False)
        clean_csv_b64 = base64.b64encode(clean_csv.encode()).decode()
        
        # Build response
        response = {
            'status': 'SUCCESS',
            'fileName': file_name,
            'folderPath': folder_path,
            'targetTable': target_table,
            'originalRowCount': original_row_count,
            'cleanRowCount': clean_row_count,
            'dirtyRowCount': dirty_row_count,
            'hasDirtyRows': dirty_row_count > 0,
            'cleanDataBase64': clean_csv_b64,
            'dirtyRowDetails': dirty_row_details,
            'columnAnalysis': column_analysis,
            'processedAt': datetime.now().isoformat()
        }
        
        return func.HttpResponse(
            json.dumps(response, default=str),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                'status': 'ERROR',
                'error': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )