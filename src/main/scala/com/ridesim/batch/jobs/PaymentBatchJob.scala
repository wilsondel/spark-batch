package com.ridesim.batch.jobs

import com.ridesim.batch.domain.Schemas
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PaymentBatchJob {

  def readPayments(spark: SparkSession, brokers: String, topic: String): DataFrame = {
    import spark.implicits._
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets",   "latest")
      .option("failOnDataLoss",  "false")
      .load()
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json($"json_str", Schemas.PaymentSchema).as("p"))
      .select("p.*")
      .filter($"status" === "SUCCESS")
  }

  def revenueByMethod(payments: DataFrame): DataFrame =
    payments
      .groupBy("method")
      .agg(
        count("*")        .as("total_rides"),
        sum("amount")     .as("total_revenue"),
        avg("amount")     .as("avg_amount"),
        min("amount")     .as("min_amount"),
        max("amount")     .as("max_amount")
      )
      .orderBy(desc("total_revenue"))

  def revenueByDriver(payments: DataFrame): DataFrame =
    payments
      .groupBy("driver_id")
      .agg(
        count("*")    .as("rides_completed"),
        sum("amount") .as("total_earned"),
        avg("amount") .as("avg_per_ride")
      )
      .orderBy(desc("total_earned"))

  def spendingByUser(payments: DataFrame): DataFrame =
    payments
      .groupBy("user_id")
      .agg(
        count("*")    .as("rides_taken"),
        sum("amount") .as("total_spent"),
        avg("amount") .as("avg_per_ride")
      )
      .orderBy(desc("total_spent"))

  def globalSummary(payments: DataFrame): DataFrame =
    payments.agg(
      count("*")              .as("total_transactions"),
      sum("amount")           .as("total_revenue"),
      avg("amount")           .as("avg_ride_price"),
      min("amount")           .as("min_ride_price"),
      max("amount")           .as("max_ride_price"),
      countDistinct("user_id")    .as("unique_users"),
      countDistinct("driver_id")  .as("unique_drivers"),
      countDistinct("trip_id")    .as("unique_trips")
    )
}
