# Airflow image + what the pipeline needs that the base image lacks.
# The base image already has pandas 2.1.4 and psycopg2-binary 2.9.10, which
# requirements.txt pins to match, so local runs and Airflow use the same versions.
FROM apache/airflow:2.10.5-python3.11

RUN pip install --no-cache-dir pytest==8.3.4
