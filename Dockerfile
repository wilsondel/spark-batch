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
    chown -R spark:spark /opt/app /opt/output
COPY --from=builder /tmp/app.jar /opt/app/app.jar
RUN chown spark:spark /opt/app/app.jar
USER spark
WORKDIR /opt/app

ENV SPARK_MASTER=local[*] \
    KAFKA_BROKERS=kafka:9092 \
    TOPIC_PAYMENT=topic.payment \
    TOPIC_RATING=topic.rating \
    OUTPUT_DIR=/opt/output \
    OUTPUT_FORMAT=json

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--master", "local[*]", \
  "--conf", "spark.driver.memory=1g", \
  "--conf", "spark.executor.memory=1g", \
  "--class", "com.ridesim.batch.Main", \
  "/opt/app/app.jar"]
