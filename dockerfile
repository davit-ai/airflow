# Use official Airflow 3.0 image
FROM apache/airflow:3.0.0-python3.12

# Install required packages
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    dos2unix && \
    rm -rf /var/lib/apt/lists/*

# Copy and prepare entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Create required directories with correct permissions
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs && \
    chown -R airflow: /opt/airflow

USER airflow

ENTRYPOINT ["/entrypoint.sh"]
