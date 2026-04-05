# 1. Basis-Image verwenden (3.12-slim spart Platz)
FROM apache/airflow:latest-python3.12

# 2. Pip upgraden
RUN pip install --no-cache-dir --upgrade pip

# 3. Requirements kopieren und installieren
# Wir kopieren erst nur die Requirements, um den Docker-Cache optimal zu nutzen
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt