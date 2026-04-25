#!/bin/bash
set -e

# The Kafka broker advertises 3.89.32.179:9092 but the reachable address is 54.209.228.100:9092.
# Redirect all traffic to the advertised IP toward the actual reachable IP.
KAFKA_ADVERTISED_IP="${KAFKA_ADVERTISED_IP:-3.89.32.179}"
KAFKA_REAL_IP=$(echo "${KAFKA_BROKERS:-54.209.228.100:9092}" | cut -d: -f1)

iptables -t nat -A OUTPUT -d "${KAFKA_ADVERTISED_IP}/32" -p tcp --dport 9092 \
  -j DNAT --to-destination "${KAFKA_REAL_IP}:9092" 2>/dev/null || true

exec /opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --class com.ridesim.batch.Main \
  /opt/app/app.jar
