#!/bin/bash

# Set defaults
AIRFLOW_USERNAME=${AIRFLOW_USERNAME:-airflow}
AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD:-airflow}
AIRFLOW_EMAIL=${AIRFLOW_EMAIL:-admin@example.com}

# Generate secrets if not provided
if [ -z "$AIRFLOW__CORE__FERNET_KEY" ]; then
    echo "Generating new Fernet key"
    export AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
fi

if [ -z "$AIRFLOW__WEBSERVER__SECRET_KEY" ]; then
    echo "Generating new secret key"
    export AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 30)
fi

# Initialize database (Airflow 3.0 command)
airflow db migrate

# Create user (Airflow 3.0 command)
airflow users create \
    --username "$AIRFLOW_USERNAME" \
    --password "$AIRFLOW_PASSWORD" \
    --firstname "Admin" \
    --lastname "User" \
    --role Admin \
    --email "$AIRFLOW_EMAIL"

# Start services (Airflow 3.0 commands)
airflow api-server &
airflow scheduler