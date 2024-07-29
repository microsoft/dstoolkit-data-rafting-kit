FROM ubuntu:24.04

USER root

RUN apt-get update && yes | unminimize

RUN apt-get update \
    && apt-get install -y \
    git \
    wget \
    curl \
    sudo \
    locales \
    scala \
    python3 \
    python3-pip \
    python3-venv \
    python3-poetry \
    build-essential \
    libhdf5-dev \
    libsnappy-dev \
    libzstd-dev \
    libbz2-dev \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3.3.6 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

# Download and uncompress spark from the apache archive
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && mkdir -p /opt/spark \
    && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
    && rm apache-spark.tgz

# Download and install Hadoop native libraries
RUN wget --no-verbose -O hadoop-native.tgz "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    && tar -xf hadoop-native.tgz \
    && cp -r hadoop-${HADOOP_VERSION}/lib/native/* /usr/local/lib/ \
    && rm -rf hadoop-${HADOOP_VERSION} hadoop-native.tgz

# Ensure the native libraries are found
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
ENV PATH=$SPARK_HOME/bin:$PATH

# Verify installation
RUN java -version && javac -version && python3 --version

WORKDIR /opt/spark
