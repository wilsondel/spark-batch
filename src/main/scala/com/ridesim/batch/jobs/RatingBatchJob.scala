package com.ridesim.batch.jobs

import com.ridesim.batch.domain.Schemas
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RatingBatchJob {

  def readRatings(spark: SparkSession, brokers: String, topic: String): DataFrame = {
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
      .select(from_json($"json_str", Schemas.RatingSchema).as("r"))
      .select("r.*")
  }

  def driverRankings(ratings: DataFrame): DataFrame =
    ratings
      .filter(col("to_role") === "DRIVER")
      .groupBy("to_id")
      .agg(
        avg("stars")  .as("avg_stars"),
        count("*")    .as("total_ratings"),
        min("stars")  .as("min_stars"),
        max("stars")  .as("max_stars")
      )
      .withColumnRenamed("to_id", "driver_id")
      .orderBy(desc("avg_stars"), desc("total_ratings"))

  def userRankings(ratings: DataFrame): DataFrame =
    ratings
      .filter(col("to_role") === "USER")
      .groupBy("to_id")
      .agg(
        avg("stars")  .as("avg_stars"),
        count("*")    .as("total_ratings"),
        min("stars")  .as("min_stars"),
        max("stars")  .as("max_stars")
      )
      .withColumnRenamed("to_id", "user_id")
      .orderBy(desc("avg_stars"), desc("total_ratings"))

  def starDistribution(ratings: DataFrame): DataFrame =
    ratings
      .groupBy("stars")
      .agg(count("*").as("count"))
      .orderBy("stars")

  def globalSummary(ratings: DataFrame): DataFrame =
    ratings.agg(
      count("*")                  .as("total_ratings"),
      avg("stars")                .as("global_avg_stars"),
      countDistinct("trip_id")    .as("trips_rated"),
      countDistinct("from_id")    .as("unique_raters")
    )
}
