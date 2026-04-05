# 1. Basis-Image verwenden (3.12-slim spart Platz)
FROM python:3.12-slim

# 2. Arbeitsverzeichnis im Container festlegen
WORKDIR /app

# 3. System-Abhängigkeiten installieren (nur falls nötig, z.B. für manche DB-Treiber)
# Falls du keine speziellen C-Libraries brauchst, kannst du diesen Block löschen:
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 4. Pip upgraden
RUN pip install --no-cache-dir --upgrade pip

# 5. Requirements kopieren und installieren
# Wir kopieren erst nur die Requirements, um den Docker-Cache optimal zu nutzen
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. (Optional) Den restlichen Code kopieren 
# Bei @task.kubernetes wird das Skript oft von Airflow injiziert, 
# aber für lokale Tests ist das hier sinnvoll:
COPY . .

# 7. Best Practice: Als Non-Root User ausführen (Sicherheit in K8s)
USER 1001

# Standard-Befehl (wird von Airflow meist überschrieben)
CMD ["python3"]