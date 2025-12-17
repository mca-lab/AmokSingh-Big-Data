# Big Data Analysis Project

### Automated Data Pipeline using Python, PySpark, Docker & Jupyter

---

## Project Overview

This project demonstrates an **end-to-end Big Data processing pipeline** using  
**Python, PySpark, Docker, and Jupyter Notebook**.

The pipeline covers:

- Data ingestion from multiple sources
- Data cleaning and integration using PySpark
- Data analysis and visualization
- Reproducible Docker-based setup

The project showcases how big data tools can be used to extract **meaningful insights**
from large, real-world datasets.

## Research Question

**How do climate indicators such as temperature, precipitation, and CO₂ emissions
influence agricultural production and food security across countries over time?**

## Datasets Used

The project integrates multiple public datasets related to:

- Climate indicators (temperature, precipitation)
- Environmental factors (CO₂ emissions)
- Agriculture (crop production, cereal yield, agricultural land)
- Food security index

## Project Workflow

### **Module 1: Data Collection & Ingestion**

**Objective:** Automate dataset downloading and storage.

- Fetches multiple public datasets dynamically
- Stores raw data in `data/raw/`
- Docker ensures reproducibility

**Deliverables:**

- `fetch_data.py`
- Dockerfile for ingestion

### **Module 2: Data Cleaning & Integration**

**Objective:** Prepare data for analysis using PySpark.

- Loads raw datasets into PySpark
- Handles missing values and inconsistencies
- Integrates multiple datasets
- Stores cleaned data in `data/processed/`
- Docker ensures reproducible processing

**Deliverables:**

- `clean_data.py`
- Dockerfile for cleaning

### **Module 3: Data Analysis & Visualization**

**Objective:** Analyze cleaned data and answer the research question.

- Loads processed data into Jupyter Notebook
- Performs descriptive statistics and aggregations
- Visualizes insights using Matplotlib and Seaborn
- Documents findings and interpretations

**Key Visualizations Include:**

- Climate trends over time
- CO₂ emissions comparison across countries
- Agricultural production analysis
- Food security index comparison
- Correlation heatmaps

**Deliverables:**

- `analysis.ipynb`

## Key Insights

- Climate indicators show clear trends over time
- CO₂ emissions and temperature correlate with agricultural outputs
- Food security varies significantly across countries
- Environmental factors strongly influence agriculture and food security

## Technologies Used

- Python
- PySpark
- Docker
- Jupyter Notebook
- Pandas
- Matplotlib
- Seaborn

## How to Run the Project

1. Run Docker container for data ingestion
2. Run Docker container for data cleaning
3. Open `notebooks/analysis.ipynb`
4. Run all cells to view analysis and visualizations

## Conclusion

This project demonstrates a **complete Big Data pipeline** from data collection
to insight generation. It highlights the use of PySpark and Docker for scalable
and reproducible data processing, and Jupyter Notebook for analysis and visualization.

## 👤 Author

**Amok Singh**  
MCA Student  
Big Data Project
