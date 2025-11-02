FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy source code
COPY src/ ./src/

# Create data directories
RUN mkdir -p data/raw data/processed

# Run data collection
CMD ["python", "src/fetch_data.py"]