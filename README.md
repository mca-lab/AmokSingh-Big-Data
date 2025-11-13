# Big Data Project — Climate & Agriculture Data Pipeline

## Overview

This project implements a Big Data pipeline that collects, cleans, and prepares environmental and agricultural datasets for further analysis.  
The work completed so far covers **Module 1 (Data Ingestion)** and **Module 2 (Data Cleaning & Integration)** from the Big Data project template.

## Module 1 — Data Collection & Ingestion

### What was done

- Fetched **real climate data** from the **World Bank API**, including:
  - Precipitation
  - CO₂ Emissions
  - Methane Emissions
- Generated a **simulated agriculture dataset** for 20 countries (FAO-style).
- Saved all raw datasets in **CSV and Parquet** formats.
- Automated the entire ingestion step inside **Docker** to ensure reproducibility.

### Output (Module 1)

data/raw/
│── climate_data.csv
│── climate_data.parquet
│── agriculture_data.csv
│── agriculture_data.parquet
└── dataset_info.json

## Module 2 — Data Cleaning & Integration (PySpark)

### What was done

- Loaded raw datasets using **PySpark**.
- Cleaned and formatted data:
  - Converted columns to proper numeric types
  - Trimmed and standardized country fields
  - Removed duplicates
- Pivoted climate indicators (long → wide format).
- Merged agriculture + climate datasets using **country + year**.
- Filled missing values using **median per country**.
- Flagged outliers using **IQR (Interquartile Range)**.
- Saved the processed dataset into **data/processed/**.

### Output (Module 2)

data/processed/
│── merged_climate_agriculture.csv
└── merged_climate_agriculture.parquet
