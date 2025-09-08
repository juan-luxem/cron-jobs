from pandas import DataFrame

def find_header_row(df: DataFrame, first_row: str, second_row: str) -> int:
    """
    Finds the row index where the headers first_row and second_row are present.
    Handles variations in spacing and quotes.
    """
    for idx, row in df.iterrows():
        if idx > 20:  # Limit search to first 20 rows
            break
        
        # Convert all values in the row to string and check
        row_values = [str(val).strip().replace('"', '').strip() for val in row.values]
        
        # Look for "Zona de carga" (with variations)
        zona_found = any(first_row in val for val in row_values)

        # Look for "Hora" (with variations in spacing)
        hora_found = any(second_row in val for val in row_values)

        if zona_found and hora_found:
            return idx
    
    return -1