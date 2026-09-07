FROM apache/airflow:2.9.0-python3.12

USER airflow

RUN pip install --no-cache-dir \
    "minio>=7.2,<8.0" \
    "psycopg2-binary>=2.9,<3.0" \
    "requests>=2.31,<3.0" \
    "python-dateutil>=2.8,<3.0"