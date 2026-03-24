from datetime import timedelta

from global_utils.parse_date import parse_date


def chunk_date_range(start_date_str, end_date_str, chunk_days=60):
    """Yields (start_str, end_str) tuples in 'DD/MM/YYYY' format."""
    start = parse_date(start_date_str)
    end = parse_date(end_date_str)

    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        yield current.strftime("%d/%m/%Y"), chunk_end.strftime("%d/%m/%Y")
        current = chunk_end + timedelta(days=1)  # Move to next day
