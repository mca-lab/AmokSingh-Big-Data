FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Create data directories
RUN mkdir -p data/raw data/processed

# Run data collection AND then cleaning
CMD ["sh", "-c", "python src/fetch_data.py && python src/clean_data.py"]