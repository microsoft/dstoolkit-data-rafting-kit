FROM ubuntu:23.04

USER root

RUN apt-get update && yes | unminimize

RUN apt-get update \
    && apt-get install -y \
        git \
        wget \
        curl \
        sudo \
        default-jdk \
        default-jre \
        locales \
        scala \
        python3 \
        python3-pip \
        python3-venv \
        python3-poetry \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

ENV SPARK_VERSION=3.5.1 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

# Download and uncompress spark from the apache archive
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz
