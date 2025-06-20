import pandas as pd
import random
import requests
import io  # Import io module
import os
from typing import Generator, Optional
from datetime import datetime, timedelta
import uuid

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
    print(f"\n📁 Output directory: '{output_dir}'")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"✅ Created output directory.")

    base_data = {}
    print("\n🔽 Downloading and loading datasets:")
    for file_name, file_url in file_links.items():
        print(f"  • Fetching {file_name}...")
        df = fetch_data_from_url(file_url)
        if not df.empty:
            base_data[file_name] = df.to_dict(orient='records')
            print(f"    → Loaded {len(df)} records.")
        else:
            print(f"    ⚠️ Failed to load {file_name}.")

    if not base_data:
        print("❌ No data found to base generation on. Aborting.")
        return

    print("\n⚙️ Starting synthetic data generation...\n")
    for file_name, records in base_data.items():
        if not records:
            print(f"⚠️ Skipping {file_name} — no records found.")
            continue

        print(f"📄 Processing file: {file_name}")

        df_original = pd.DataFrame(records)
        unique_columns = [col for col in df_original.columns if df_original[col].is_unique]
        if unique_columns:
            print(f"   → Unique columns detected: {', '.join(unique_columns)}")
        else:
            print(f"   → No unique columns detected.")

        synthetic_data = []
        if num_records:
            records_to_generate = num_records
            print(f"   → Overriding record count: {records_to_generate}")
        elif years:
            records_to_generate = int(len(records) * years)
            print(f"   → Generating {records_to_generate} records (based on {years} year(s))")
        else:
            records_to_generate = len(records)
            print(f"   → Generating default number of records: {records_to_generate}")

        synthetic_id_counters = {}

        for i in range(records_to_generate):
            base = random.choice(records)
            synthetic_record = base.copy()

            for key, value in synthetic_record.items():
                if key in unique_columns:
                    if isinstance(value, int):
                        if key not in synthetic_id_counters:
                            synthetic_id_counters[key] = max(df_original[key]) + 1
                        synthetic_record[key] = synthetic_id_counters[key]
                        synthetic_id_counters[key] += 1
                    elif isinstance(value, str) and len(value) >= 8:
                        synthetic_record[key] = str(uuid.uuid4())
                    else:
                        synthetic_record[key] = f"{key}_synthetic_{i}"
                    continue

                if isinstance(value, int):
                    if value > 1:
                        synthetic_record[key] = int(value * random.uniform(0.9, 1.1))
                elif isinstance(value, float):
                    synthetic_record[key] = value * random.uniform(0.9, 1.1)
                elif isinstance(value, str) and 'date' in key.lower():
                    synthetic_record[key] = (datetime.now() + timedelta(
                        days=random.randint(-365 * years if years else -30, 30))).strftime('%Y-%m-%d')

            synthetic_data.append(synthetic_record)

            if (i + 1) % 1000 == 0 or i == records_to_generate - 1:
                print(f"     → Generated {i + 1}/{records_to_generate} records", end='\r')

        df_synthetic = pd.DataFrame(synthetic_data)
        output_file = os.path.join(output_dir, file_name)
        df_synthetic.to_csv(output_file, index=False)
        print(f"\n✅ Synthetic data saved to: {output_file}\n")

    print("🎉 All synthetic datasets generated successfully.\n")

# Example usage of the function
if __name__ == '__main__':
    generate_synthetic_data("synthetic_output", num_records=None, years=1)
