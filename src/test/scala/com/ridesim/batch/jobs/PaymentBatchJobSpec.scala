package com.ridesim.batch.jobs

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PaymentBatchJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("PaymentBatchJobSpec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  import spark.implicits._

  private def payments = Seq(
    ("t1", "user-1", "driver-A", 10.0, "USD", "CASH",   "SUCCESS"),
    ("t2", "user-1", "driver-A", 20.0, "USD", "CASH",   "SUCCESS"),
    ("t3", "user-2", "driver-B", 15.0, "USD", "CARD",   "SUCCESS"),
    ("t4", "user-2", "driver-B", 25.0, "USD", "CARD",   "SUCCESS"),
    ("t5", "user-3", "driver-C", 50.0, "USD", "WALLET", "SUCCESS"),
    ("t6", "user-3", "driver-C",  5.0, "USD", "CASH",   "FAILED")   // must be excluded
  ).toDF("trip_id","user_id","driver_id","amount","currency","method","status")
   .filter($"status" === "SUCCESS")

  "revenueByMethod" should "aggregate correctly by payment method" in {
    val result = PaymentBatchJob.revenueByMethod(payments)
    val cash = result.filter($"method" === "CASH").head()
    cash.getAs[Long]("total_rides")      shouldBe 2L
    cash.getAs[Double]("total_revenue")  shouldBe 30.0
    cash.getAs[Double]("avg_amount")     shouldBe 15.0
  }

  it should "exclude FAILED payments" in {
    val result = PaymentBatchJob.revenueByMethod(payments)
    result.agg(org.apache.spark.sql.functions.sum("total_rides"))
      .as[Long].first() shouldBe 5L
  }

  "revenueByDriver" should "rank drivers by total earned" in {
    val result = PaymentBatchJob.revenueByDriver(payments).collect()
    result.head.getAs[String]("driver_id") shouldBe "driver-C"
    result.head.getAs[Double]("total_earned") shouldBe 50.0
    result.head.getAs[Long]("rides_completed") shouldBe 1L
  }

  "spendingByUser" should "rank users by total spent" in {
    val result = PaymentBatchJob.spendingByUser(payments).collect()
    result.head.getAs[String]("user_id") shouldBe "user-3"
    result.head.getAs[Double]("total_spent") shouldBe 50.0
  }

  "globalSummary" should "compute totals correctly" in {
    val row = PaymentBatchJob.globalSummary(payments).head()
    row.getAs[Long]("total_transactions")  shouldBe 5L
    row.getAs[Double]("total_revenue")     shouldBe 120.0
    row.getAs[Double]("avg_ride_price")    shouldBe 24.0
    row.getAs[Double]("min_ride_price")    shouldBe 10.0
    row.getAs[Double]("max_ride_price")    shouldBe 50.0
    row.getAs[Long]("unique_users")        shouldBe 3L
    row.getAs[Long]("unique_drivers")      shouldBe 3L
  }
}
