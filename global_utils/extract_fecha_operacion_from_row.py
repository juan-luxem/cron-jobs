import re
import logging
from typing import Optional

def extract_fecha_operacion_from_row(content: str, search_text: str) -> Optional[str]:
    """
    Extracts fecha_operacion from CSV content by searching for a specific text pattern.
    
    Args:
        content: The CSV file content as string
        search_text: The text to search for (e.g., "Fecha de Publicacion:")
    
    Returns:
        Date in YYYY-MM-DD format or None if not found
        
    Examples:
        content = '"Fecha de Publicacion: 12/oct/2025"'
        result = extract_fecha_operacion_from_row(content, "Fecha de Publicacion:")
        # Returns: "2025-10-12"
        
        content = '"Archivo descargado desde el Sistema ... creado el 12/oct/2025 20:00:01 hrs."'
        result = extract_fecha_operacion_from_row(content, "creado el")
        # Returns: "2025-10-12"
    """
    
    # Spanish month mapping
    spanish_months = {
        'ene': '01', 'enero': '01',
        'feb': '02', 'febrero': '02', 
        'mar': '03', 'marzo': '03',
        'abr': '04', 'abril': '04',
        'may': '05', 'mayo': '05',
        'jun': '06', 'junio': '06',
        'jul': '07', 'julio': '07',
        'ago': '08', 'agosto': '08',
        'sep': '09', 'septiembre': '09', 'sept': '09',
        'oct': '10', 'octubre': '10',
        'nov': '11', 'noviembre': '11',
        'dic': '12', 'diciembre': '12'
    }
    
    try:
        # Split content into lines
        lines = content.split('\n')
        
        # Search for the line containing the search text
        target_line = None
        for line in lines:
            if search_text.lower() in line.lower():
                target_line = line
                break
        
        if not target_line:
            logging.error(f"Could not find line containing '{search_text}' in content")
            return None
        
        logging.info(f"Found target line: {target_line.strip()}")
        
        # Look for date patterns in the line
        # Pattern 1: DD/MMM/YYYY (e.g., "12/oct/2025")
        pattern1 = re.search(r'(\d{1,2})/([a-zA-Z]{3,9})/(\d{4})', target_line)
        if pattern1:
            day, month_name, year = pattern1.groups()
            month_name_lower = month_name.lower()
            
            if month_name_lower in spanish_months:
                month = spanish_months[month_name_lower]
                formatted_date = f"{year}-{month}-{day.zfill(2)}"
                logging.info(f"Extracted date: {formatted_date}")
                return formatted_date
            else:
                logging.error(f"Unknown month name: {month_name}")
                return None
        
        # Pattern 2: DD-MMM-YYYY (e.g., "12-oct-2025")
        pattern2 = re.search(r'(\d{1,2})-([a-zA-Z]{3,9})-(\d{4})', target_line)
        if pattern2:
            day, month_name, year = pattern2.groups()
            month_name_lower = month_name.lower()
            
            if month_name_lower in spanish_months:
                month = spanish_months[month_name_lower]
                formatted_date = f"{year}-{month}-{day.zfill(2)}"
                logging.info(f"Extracted date: {formatted_date}")
                return formatted_date
            else:
                logging.error(f"Unknown month name: {month_name}")
                return None
        
        # Pattern 3: DD de MMM de YYYY (e.g., "12 de octubre de 2025")
        pattern3 = re.search(r'(\d{1,2})\s+de\s+([a-zA-Z]{3,9})\s+de\s+(\d{4})', target_line)
        if pattern3:
            day, month_name, year = pattern3.groups()
            month_name_lower = month_name.lower()
            
            if month_name_lower in spanish_months:
                month = spanish_months[month_name_lower]
                formatted_date = f"{year}-{month}-{day.zfill(2)}"
                logging.info(f"Extracted date: {formatted_date}")
                return formatted_date
            else:
                logging.error(f"Unknown month name: {month_name}")
                return None
        
        # Pattern 4: Already formatted YYYY-MM-DD
        pattern4 = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', target_line)
        if pattern4:
            year, month, day = pattern4.groups()
            formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            logging.info(f"Found already formatted date: {formatted_date}")
            return formatted_date
        
        logging.error(f"No recognizable date pattern found in line: {target_line}")
        return None
        
    except Exception as e:
        logging.error(f"Error extracting date from content with search text '{search_text}': {e}")
        return None