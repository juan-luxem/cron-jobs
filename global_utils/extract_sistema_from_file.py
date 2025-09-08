import re
import logging

def extract_sistema_from_filename(filename: str) -> str | None:
    """
    Extracts the Sistema (SIN, BCS, BCA) from a filename by looking for it 
    as a whole word.

    Args:
        filename: The name of the file to process.

    Returns:
        The system identifier ('SIN', 'BCS', or 'BCA') if found, 
        otherwise 'UNKNOWN'.
    
    Examples:
    >>> extract_sistema_from_filename("CantAsigEnerElecZonCarMDA SIN MDA...")
    'SIN'
    >>> extract_sistema_from_filename("PreciosMargLocales BCS MDA Dia...")
    'BCS'
    >>> extract_sistema_from_filename("Cap Transferencia BCA Periodo...")
    'BCA'
    >>> extract_sistema_from_filename("Some other file without a system")
    'UNKNOWN'
    """
    # Look for SIN, BCS, or BCA as a whole word (\b is a word boundary)
    pattern = r'\b(SIN|BCS|BCA)\b'
    
    match = re.search(pattern, filename)
    
    if match:
        # Return the first captured group, which is the system name
        return match.group(1)
    else:
        logging.warning(f"Could not extract Sistema from filename: {filename}")
        return None