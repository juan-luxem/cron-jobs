import logging
import re
import urllib.parse


def extract_viewstate(response_text: str, url_encode: bool = False) -> str:
    """
    Extract VIEWSTATE value from ASP.NET response text.
    
    Args:
        response_text (str): The response text containing the VIEWSTATE value.
        url_encode (bool): If True, URL-encode the extracted VIEWSTATE value.
        
    Returns:
        str: The extracted VIEWSTATE value (optionally URL-encoded), or empty string if not found.
    """
    match = re.search(r"\|hiddenField\|__VIEWSTATE\|([^|]+)", response_text)
    
    if match:
        viewstate_value = match.group(1)
        if url_encode:
            return urllib.parse.quote_plus(viewstate_value, safe="")
        return viewstate_value
    else:
        logging.warning("VIEWSTATE not found in response")
        return ""
