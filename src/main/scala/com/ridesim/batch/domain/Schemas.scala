package com.ridesim.batch.domain

import org.apache.spark.sql.types._

object Schemas {

  val PaymentSchema: StructType = StructType(Seq(
    StructField("event_id",       StringType,  nullable = true),
    StructField("timestamp_utc",  StringType,  nullable = true),
    StructField("trip_id",        StringType,  nullable = true),
    StructField("user_id",        StringType,  nullable = true),
    StructField("driver_id",      StringType,  nullable = true),
    StructField("amount",         DoubleType,  nullable = true),
    StructField("currency",       StringType,  nullable = true),
    StructField("method",         StringType,  nullable = true),
    StructField("status",         StringType,  nullable = true),
    StructField("schema_version", StringType,  nullable = true)
  ))

  val RatingSchema: StructType = StructType(Seq(
    StructField("event_id",       StringType,  nullable = true),
    StructField("timestamp_utc",  StringType,  nullable = true),
    StructField("trip_id",        StringType,  nullable = true),
    StructField("from_id",        StringType,  nullable = true),
    StructField("from_role",      StringType,  nullable = true),
    StructField("to_id",          StringType,  nullable = true),
    StructField("to_role",        StringType,  nullable = true),
    StructField("stars",          IntegerType, nullable = true),
    StructField("comment",        StringType,  nullable = true),
    StructField("schema_version", StringType,  nullable = true)
  ))
}
