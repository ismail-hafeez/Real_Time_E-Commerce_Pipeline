# docker/spark/Dockerfile
FROM bitnami/spark:3.5.1

USER root

# 1. Install Python packages required for your PySpark scripts
RUN pip install --no-cache-dir \
    boto3 \
    kafka-python \
    requests \
    psycopg2-binary

# 2. Download and install all required integration JARs into Spark's class path
# We match these versions specifically to Spark 3.5.1 (which runs on Scala 2.12 and Hadoop 3)

# AWS S3 Data Lake Connectors
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/bitnami/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/bitnami/spark/jars/

# Apache Kafka Streaming Connectors
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12.jar /opt/bitnami/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12.jar /opt/bitnami/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/bitnami/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/bitnami/spark/jars/

# PostgreSQL Data Warehouse Connector (JDBC Driver)
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar /opt/bitnami/spark/jars/

# 3. Revert back to the default non-root Bitnami Spark user (security best practice)
USER 1001