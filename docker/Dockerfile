FROM apache/spark:3.5.5

USER root

# Connecteurs S3A (MinIO) alignes sur le Hadoop embarque de Spark 3.5.x (Hadoop 3.3.4)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

COPY pipeline/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/app
COPY pipeline/ /opt/app/pipeline/
COPY scripts/ /opt/app/scripts/

ENV PYTHONPATH=/opt/app:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYSPARK_SUBMIT_ARGS="--driver-memory 2g pyspark-shell" \
    PYTHONUNBUFFERED=1
