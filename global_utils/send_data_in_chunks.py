import requests
import logging
from typing import Dict, List

def send_data_in_chunks(data: List[Dict], endpoint_url: str, chunk_size: int = 2000) -> bool:
    """
    Sends data to API in chunks to handle large datasets.
    
    Args:
        data: List of dictionaries to send
        endpoint_url: The API endpoint URL
        chunk_size: Number of records per chunk (default: 2000)
    
    Returns:
        bool: True if all chunks sent successfully, False otherwise
    """
    total_chunks = (len(data) + chunk_size - 1) // chunk_size
    logging.info(f"Sending {len(data)} records in {total_chunks} chunks of {chunk_size}")
    
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        chunk_number = (i // chunk_size) + 1
        
        try:
            response = requests.post(
                endpoint_url,
                json=chunk,
                headers={"Content-Type": "application/json"},
                timeout=60
            )
            
            if response.status_code in [200, 201]:
                logging.info(f"✅ Chunk {chunk_number}/{total_chunks} sent successfully ({len(chunk)} records)")
            else:
                logging.error(f"❌ Failed to send chunk {chunk_number}: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Network error sending chunk {chunk_number}: {e}")
            return False
        except Exception as e:
            logging.error(f"❌ Unexpected error sending chunk {chunk_number}: {e}")
            return False
    
    logging.info(f"🎉 All {total_chunks} chunks sent successfully!")
    return True
