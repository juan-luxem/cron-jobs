from pandas import Index
from typing import List

def clean_column_names(columns: Index[str]) -> List[str]:
    """
    Cleans column names by removing quotes and extra spaces.
    """
    cleaned = []
    for col in columns:
        # Remove quotes and strip whitespace
        clean_col = str(col).strip().replace('"', '').strip()
        cleaned.append(clean_col)
    return cleaned