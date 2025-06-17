import pandas as pd
import random
import requests
import io  # Import io module
import os
from typing import Generator, Optional
from datetime import datetime, timedelta

# JSON data with file links
file_links = {
    "battery.csv": "https://zenodo.org/api/records/14751777/files/battery.csv/content",
    "measurements_airquality.csv": "https://zenodo.org/api/records/14751777/files/measurements_airquality.csv/content",
    "measurements_basic.csv": "https://zenodo.org/api/records/14751777/files/measurements_basic.csv/content",
    "measurements_cut_processed.csv": "https://zenodo.org/api/records/14751777/files/measurements_cut_processed.csv/content",
    "measurements_cut_ws.csv": "https://zenodo.org/api/records/14751777/files/measurements_cut_ws.csv/content",
    "measurements_dust.csv": "https://zenodo.org/api/records/14751777/files/measurements_dust.csv/content",
    "measurements_meteorology.csv": "https://zenodo.org/api/records/14751777/files/measurements_meteorology.csv/content",
    "measurements_oil_detectors.csv": "https://zenodo.org/api/records/14751777/files/measurements_oil_detectors.csv/content"
}


def fetch_data_from_url(url: str) -> pd.DataFrame:
    """Fetch CSV content from URL and return as DataFrame."""
    response = requests.get(url)
    if response.status_code == 200:
        return pd.read_csv(io.StringIO(response.text))  # Use io.StringIO instead of pd.compat.StringIO
    else:
        print(f"Failed to fetch {url}, status code: {response.status_code}")
        return pd.DataFrame()


def generate_synthetic_data(output_dir: str, num_records: Optional[int] = None, years: Optional[int] = None):
    """Generates synthetic data based on patterns from online datasets and writes it to multiple CSV files."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # Ensure output directory exists

    base_data = {}
    for file_name, file_url in file_links.items():
        df = fetch_data_from_url(file_url)
        if not df.empty:
            base_data[file_name] = df.to_dict(orient='records')

    if not base_data:
        print("No data found to base generation on.")
        return

    for file_name, records in base_data.items():
        if not records:
            print(f"Skipping {file_name} as no records were fetched.")
            continue

        synthetic_data = []
        if num_records:
            records_to_generate = num_records
        elif years:
            records_to_generate = int(len(records) * years)  # Ensure integer conversion
        else:
            records_to_generate = len(records)  # Default to same amount as input file

        for _ in range(records_to_generate):
            base = random.choice(records)
            synthetic_record = base.copy()
            # Random variations based on numeric fields
            for key, value in synthetic_record.items():
                if isinstance(value, (int, float)):
                    synthetic_record[key] = value * random.uniform(0.9, 1.1)
                elif isinstance(value, str) and 'date' in key.lower():
                    synthetic_record[key] = (datetime.now() + timedelta(
                        days=random.randint(-365 * years if years else -30, 30))).strftime('%Y-%m-%d')
            synthetic_data.append(synthetic_record)

        # Convert to DataFrame and save to file
        df_synthetic = pd.DataFrame(synthetic_data)
        output_file = os.path.join(output_dir, file_name)
        df_synthetic.to_csv(output_file, index=False)
        print(f"Synthetic data saved to {output_file}")


# Example usage of the function
if __name__ == '__main__':
    generate_synthetic_data("synthetic_output", num_records=None, years=6)
