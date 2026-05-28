# docker/producer/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (curl is useful for testing network connections)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install required Python libraries:
# - kafka-python: To send messages to Kafka
# - pandas & openpyxl: To read the Excel/Parquet dataset
# - requests: To fetch the live currency data
RUN pip install --no-cache-dir \
    kafka-python-ng \
    pandas \
    openpyxl \
    boto3 \
    requests

# Copy the producer source code and helper utilities into the container
COPY ./src/producers /app/producers
COPY ./src/utils /app/utils

# Run the order simulator script by default
CMD ["python", "producers/order_producer.py"]