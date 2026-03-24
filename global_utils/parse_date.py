from datetime import datetime


def parse_date(date_str):
    """Parses YYYY-MM-DD (API) or DD/MM/YYYY (Site) into a datetime object."""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Date format not recognized: {date_str}")
