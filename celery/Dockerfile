FROM apache/airflow:2.7.1

USER root

# Install OpenJDK-11 (no recommended extras, no apt lists left in the image)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/lib/jvm/java-11-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-11-openjdk

# Set JAVA_HOME (an architecture-neutral link, so arm64 hosts work too)
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk/

USER airflow

WORKDIR /app

#COPY /variables/airflow_variables.json /app
#COPY /variables/airflow_connections.json /app