# Synthetic Environmental Dataset Generator

This Python script fetches real-world environmental measurement datasets from Zenodo and generates synthetic data based on them. It is useful for testing, simulations, or training machine learning models where access to extended or customizable datasets is required.

---

## 📦 Features

- Downloads multiple environmental datasets (air quality, dust, meteorology, etc.)
- Generates synthetic versions of those datasets
- Supports:
  - Custom number of records
  - Scaling based on a number of years
- Outputs to easily reusable CSV files

---

## 📂 Source Datasets

The script uses datasets from this Zenodo record:  
🔗 https://zenodo.org/record/14751777

Fetched files include:
- `battery.csv`
- `measurements_airquality.csv`
- `measurements_basic.csv`
- `measurements_cut_processed.csv`
- `measurements_cut_ws.csv`
- `measurements_dust.csv`
- `measurements_meteorology.csv`
- `measurements_oil_detectors.csv`

---

## 🚀 Usage

### 1. Clone the repository
```bash
git clone https://github.com/Rinnoco/data-generator.git
cd data-generator
