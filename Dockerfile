FROM apache/airflow:2.10.1

USER root

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

# Install Spark
RUN curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark-3.5.0-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN mkdir -p $SPARK_HOME/jars && \
    curl -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.565.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar

USER airflow

# Install pyspark
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    boto3 \
    pyyaml \
    requests

WORKDIR /home/airflow/
