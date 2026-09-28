FROM apache/airflow:3.0.0

# Dependencias adicionales de la Clase 7 (Dbt + adapter de Snowflake + Cosmos)
# se instalan igual que el resto: todas viven en requirements.txt para que
# "docker compose build" sea la unica fuente de verdad del ambiente.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
