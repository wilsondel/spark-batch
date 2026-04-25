# syntax=docker/dockerfile:1

FROM sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.9_2.12.21 AS builder
WORKDIR /app
COPY project project
COPY build.sbt .
RUN sbt -Dsbt.ci=true update
COPY src src
RUN sbt -Dsbt.ci=true clean assembly && \
    cp /app/target/scala-2.12/spark-batch-assembly.jar /tmp/app.jar

FROM apache/spark:3.5.1
USER root
RUN mkdir -p /opt/app /opt/output && \
    apt-get update -qq && apt-get install -y --no-install-recommends iptables && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /tmp/app.jar /opt/app/app.jar
COPY entrypoint.sh /opt/app/entrypoint.sh
RUN chmod +x /opt/app/entrypoint.sh
WORKDIR /opt/app

ENV SPARK_MASTER=local[*] \
    KAFKA_BROKERS=54.209.228.100:9092 \
    KAFKA_ADVERTISED_IP=3.89.32.179 \
    TOPIC_PAYMENT=topic.payment \
    TOPIC_RATING=topic.rating \
    OUTPUT_DIR=/opt/output \
    OUTPUT_FORMAT=json \
    POSTGRES_HOST=54.209.228.100 \
    POSTGRES_DB=ridesim \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres

ENTRYPOINT ["/opt/app/entrypoint.sh"]
