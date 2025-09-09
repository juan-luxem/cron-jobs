import re
import logging

def extract_fecha_operacion_from_filename(filename: str, index: int = 3) -> str:
    """
    Extracts the fecha_operacion from the filename.
    The date is always 2 words after the Sistema (SIN, BCS, BCA).
    
    Examples:
    - "PreciosMargLocales SIN MDA Dia 2025-09-08 v2025..." -> "2025-09-08"
    - "PreciosMargLocales BCA MTR_Expost Dia 2025-09-04 v2025..." -> "2025-09-04"
    - "PreciosServiciosConexos BCA MDA Dia 2025-09-08 v2025..." -> "2025-09-08"
    """
    try:
        # Split filename into words
        words = filename.split()
        
        # Find the Sistema position
        sistema_index = -1
        for i, word in enumerate(words):
            if word in ['SIN', 'BCS', 'BCA']:
                sistema_index = i
                break
        
        if sistema_index == -1:
            logging.error(f"Could not find Sistema in filename: {filename}")
            return None
            
        # Date should be 3 positions after Sistema (Sistema + 2 words + date)
        date_index = sistema_index + index

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
