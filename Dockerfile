
# NOTE: To apply changes in the Dockerfile — such as adding RUN pip install --no-cache-dir pandas==2.1.4 — you must rebuild the image. 
# Otherwise, docker-compose up -d will simply restart containers using the existing image without applying the change.

# For airflow
# docker build -t custom-airflow:2.8.1 -f Dockerfile .

# For Kafka topic init (from the util folder)
# docker build -t kafka-topic-init -f util/Dockerfile.kafka-init .

# This project has 2 Dockerfile: Dockerfile ? for the main/default image (Airflow)
#                                util/Dockerfile.kafka-init ? for the Kafka DLQ topic creator script

# This is a Base image for Airflow
FROM apache/airflow:2.8.1-python3.11

USER root

# Install utilities
RUN apt-get update && \
    apt-get install -y sudo cifs-utils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create the mount directory
RUN mkdir -p /mnt/c/wsl-shares/bulk_files

# Switch to root user to install system-level dependencies
USER root

# Install system dependencies for pyodbc, SQL Server ODBC driver, and PostgreSQL
RUN apt-get update && apt-get install -y \
    gcc \
	g++	\
    unixodbc \
    unixodbc-dev \
    build-essential \
	freetds-dev \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17
	
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools

# Switch back to airflow user
USER airflow

# Install pyodbc
RUN pip install --no-cache-dir pyodbc

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir --upgrade apache-airflow-providers-openlineage==1.8.0

RUN pip install --no-cache-dir kafka-python faker

# RUN pip install --force-reinstall -v "pandas==1.5.3"
# Downgrade pandas to fix pyodbc compatibility
RUN pip install --force-reinstall --no-cache-dir pandas==2.1.4

# Set the working directory
WORKDIR /opt/airflow





