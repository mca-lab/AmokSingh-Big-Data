FROM python:3.10-slim

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app

# Install OS packages (java + certificates)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN mkdir -p /app/data/raw /app/data/processed \
    && chown -R app:app /app

USER app

CMD ["bash", "-lc", "python src/fetch_data.py --out /app/data && python src/clean_data.py"]
