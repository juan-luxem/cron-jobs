import re
import logging

def extract_fecha_operacion_from_filename(filename: str, index: int = 3, word: str = None) -> str | None:
    """
    Extracts the fecha_operacion from the filename.
    The date is always a certain number of positions after the Sistema (SIN, BCS, BCA) or a specified word.
    
    Args:
        filename: The filename to extract date from
        index: Number of positions after the reference word to find the date
        word: Optional specific word to look for instead of Sistema words
    
    Examples:
    - "PreciosMargLocales SIN MDA Dia 2025-09-08 v2025..." -> "2025-09-08" (default behavior)
    - "Salidas En Adelanto SEN Dia 2025-10-13 v2025..." -> "2025-10-13" (with word="Dia", index=1)
    """
    try:
        # Split filename into words
        words = filename.split()
        
        # Find the reference word position
        reference_index = -1
        
        if word:
            # Look for the specific word
            for i, w in enumerate(words):
                if w == word:
                    reference_index = i
                    break
            
            if reference_index == -1:
                logging.error(f"Could not find word '{word}' in filename: {filename}")
                return None
        else:
            # Find the Sistema position (default behavior)
            for i, w in enumerate(words):
                if w in ['SIN', 'BCS', 'BCA']:
                    reference_index = i
                    break
            
            if reference_index == -1:
                logging.error(f"Could not find Sistema in filename: {filename}")
                return None
            
        # Date should be at the specified index after the reference word
        date_index = reference_index + index

        if date_index < len(words):
            # Extract date and validate format
            date_str = words[date_index]
            # Validate date format YYYY-MM-DD
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                return date_str
            else:
                logging.error(f"Invalid date format found: {date_str} in filename: {filename}")
                return None
        else:
            logging.error(f"Could not extract date from filename: {filename}")
            return None
            
    except Exception as e:
        logging.error(f"Error extracting date from filename {filename}: {e}")
        return None
