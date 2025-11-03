# Climate & Agriculture Data Pipeline Project

## Project Overview

This Big Data project implements a complete **data pipeline** for **climate and agriculture data analysis**.  
It demonstrates **automated data collection, cleaning, and integration** using Python and Docker — preparing datasets for future analytical and visualization tasks.

**Research Question:**  
How do climate factors (temperature, precipitation, CO₂ emissions) correlate with agricultural productivity metrics?

## Project Workflow

### **Module 1: Data Collection & Ingestion** _Completed_

**Objective:** Automate dataset creation and storage.  
**Technology:** Python, Docker

**Output:** Synthetic datasets (2000–2022)

**Datasets Created:**
**Climate Data:** 1,035 records (temperature, precipitation, CO₂ emissions)  
**Agriculture Data:** 345 records (crop production, cereal yield, food security metrics)

**Countries Covered (15):**  
USA, India, China, Brazil, Germany, France, Japan, Australia, Canada, Mexico, UK, Italy, Spain, Russia, South Africa

**Time Period:** 2000–2022 (23 years)

### **Module 2: Data Cleaning & Integration** _Completed_

**Objective:** Prepare raw data for analysis.  
**Technology:** Pandas, Docker

**Output:** Cleaned and integrated dataset ready for analysis.

**Cleaning Steps:**

- Removed duplicates and null values
- Transformed climate data from long → wide format
- Integrated climate and agriculture datasets

**Final Dataset:** `345 records × 9 columns`

## Technologies Used

- **Python 3.9**
- **Pandas** — Data manipulation and analysis
- **Docker** — Containerization and reproducibility
- **PyArrow** — Parquet file support
