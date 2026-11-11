# One image for the Spark master, the workers and the driver (stream-processor).
# PySpark requires the driver and the executors to run the same Python version with the
# same libraries, and a shared image guarantees that.
FROM apache/spark:4.0.1-python3

USER root
ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# Kafka connector for Structured Streaming, baked into the image so nothing is
# downloaded at job start (versions follow Spark 4.0.1's own dependency tree).
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
 && curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
 && curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
 && chmod 644 /opt/spark/jars/*.jar

COPY docker/requirements-spark.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Project code on PYTHONPATH. The driver and the executors' Python workers inherit it
# (Spark merges the environment's PYTHONPATH into the worker path).
ENV PYTHONPATH=/opt/streampulse
WORKDIR /opt/streampulse
COPY config ./config
COPY producer ./producer
COPY detection ./detection
COPY streaming ./streaming
RUN mkdir -p /opt/streampulse/checkpoints \
 && chown -R spark:spark /opt/streampulse \
 && python3 -c "import config, detection; detection.DetectionEngine.from_yaml()"

USER spark
