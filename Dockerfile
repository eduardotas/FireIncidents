FROM apache/airflow:2.6.2

USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar -P /opt/spark/jars/

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==2.6.2" -r /requirements.txt