import requests
import json
import os
import time
from datetime import datetime, timedelta
from urllib.parse import urlencode

# Configuration
API_URL = "https://api.data.gov.in/resource/ec58dab7-d891-4abb-936e-d5d274a6ce9b"
API_KEY = "<your-api-key>"  # Replace with your actual API key
LIMIT = 1000  # Number of records per request (max likely 1000)
MAX_RESULT_WINDOW = 10000  # API limit
REQUEST_DELAY = 1  # Seconds between requests to avoid rate-limiting
START_YEAR = 2014
END_YEAR = 2024

def fetch_records_by_date():
    session = requests.Session()
    processed_records = 0
    total_records = 0  # Will be updated from API response
    current_output_file = None
    current_year = None
    first_record_in_file = True
    
    try:
        # Iterate over each day from START_YEAR to END_YEAR
        start_date = datetime(START_YEAR, 1, 1)
        end_date = datetime(END_YEAR, 12, 31)
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            year = current_date.year
            offset = 0
            
            # Initialize new output file for a new year
            if year != current_year:
                if current_output_file is not None:
                    # Close the previous year's JSON array
                    with open(current_output_file, "a", encoding="utf-8") as f:
                        f.write("\n]")
                current_year = year
                current_output_file = f"{current_year}.json"
                first_record_in_file = True
                if os.path.exists(current_output_file):
                    print(f"Warning: {current_output_file} already exists and will be overwritten.")
                with open(current_output_file, "w", encoding="utf-8") as f:
                    f.write("[\n")  # Start JSON array
            
            print(f"\nFetching records for {date_str}")
            
            while True:
                params = {
                    "api-key": API_KEY,
                    "format": "json",
                    "offset": offset,
                    "limit": LIMIT,
                    "filters[date_of_registration]": date_str
                }
                
                # Construct and print the full URL
                url = f"{API_URL}?{urlencode(params)}"
                print(f"Fetching URL: {url}")
                
                try:
                    response = session.get(API_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    records = data.get("records", [])
                    total_records = data.get("total", total_records)  # Update total if available
                    
                    if not records:
                        print(f"No more records for {date_str} at offset {offset}")
                        break
                    
                    # Write records to year-specific file
                    with open(current_output_file, "a", encoding="utf-8") as f:
                        for i, record in enumerate(records):
                            if not first_record_in_file:
                                f.write(",\n")
                            json.dump(record, f, ensure_ascii=False)
                            first_record_in_file = False
                    
                    processed_records += len(records)
                    print(f"Processed {processed_records}/{total_records or 'unknown'} records for {date_str} (offset: {offset})")
                    
                    if offset + LIMIT >= MAX_RESULT_WINDOW:
                        print(f"Warning: Reached max_result_window for {date_str}. Some records may be missing due to API limit.")
                        break
                    
                    offset += LIMIT
                    time.sleep(REQUEST_DELAY)
                
                except requests.exceptions.HTTPError as e:
                    print(f"HTTP Error fetching {date_str} at offset {offset}: {e}")
                    print(f"Response content: {response.text}")
                    time.sleep(5)  # Retry after delay
                    continue
                except requests.exceptions.JSONDecodeError as e:
                    print(f"JSON Decode Error fetching {date_str} at offset {offset}: {e}")
                    print(f"Response content: {response.text}")
                    time.sleep(5)
                    continue
                except requests.exceptions.RequestException as e:
                    print(f"Request Error fetching {date_str} at offset {offset}: {e}")
                    time.sleep(5)
                    continue
            
            # Move to next day
            current_date += timedelta(days=1)
        
        # Close the final year's JSON array
        if current_output_file is not None:
            with open(current_output_file, "a", encoding="utf-8") as f:
                f.write("\n]")
        
        print(f"\nSaved {processed_records}/{total_records or 'unknown'} records across {START_YEAR}-{END_YEAR} files")
        if total_records and processed_records < total_records:
            print("Note: Some records may be missing due to max_result_window limit. Check if additional filters are supported by the API.")
    
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    fetch_records_by_date()