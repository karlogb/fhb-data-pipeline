# fhb-data-pipeline

ETL pipeline for extracting, transforming, and presenting product data from the FHB API using Python and Polars.

## 🔍 Description

This repository contains a lightweight and modular ETL (Extract-Transform-Load) pipeline for ingesting product data from the [FHB API](https://fhb.sk), transforming it with schema normalization and deduplication, and presenting it in a current-state format using [Polars](https://www.pola.rs/).

### Features

- Extracts product data via authenticated requests from FHB API
- Transforms and stores raw data in Parquet format with inferred schemas
- Flags the most current product records
- Built for easy extension and automation

## 🧩 Requirements

- Python 3.8+
- [Polars](https://www.pola.rs/)
- pytz

Install dependencies:

```bash
pip install polars pytz requests
```

## 🗂️ Project Structure

```
fhb-data-pipeline/
│
├── src/
│   ├── extract_fhb_products.py     # Extraction + preprocessing of raw product data
│   └── transform_fhb_products.py   # Deduplication and current product flagging
│
└── README.md                      # You’re reading it!
```

## 🛠️ Setup

1. Clone the repository

```bash
git clone https://github.com/yourusername/fhb-data-pipeline.git
cd fhb-data-pipeline
```

2. Open the `extract_fhb_products.py` file and manually add your FHB API credentials
   in the `get_auth()` function:

```python
def get_auth():
    return {
        "app_id": "your_app_id",
        "secret": "your_secret"
    }
```

## 🚀 Usage

Run the extraction script:

```bash
python src/extract_fhb_products.py
```

Then run the transformation script:

```bash
python src/transform_fhb_products.py
```

## 📦 Output

The processed data is saved in the following structure:

```
/home/data/
├── landing/
├── preprocess/
└── present/
```
