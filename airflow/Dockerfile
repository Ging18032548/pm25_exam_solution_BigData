FROM apache/airflow:2.10.4-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        fonts-thai-tlwg \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}
ENV SPARK_LOCAL_IP=127.0.0.1

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
