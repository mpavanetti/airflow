FROM apache/airflow:2.7.1

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

WORKDIR /app

#COPY /variables/airflow_variables.json /app
#COPY /variables/airflow_connections.json /app