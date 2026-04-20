package com.ridesim.batch.jobs

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RatingBatchJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("RatingBatchJobSpec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  import spark.implicits._

  private def ratings = Seq(
    // user-1 → driver-A: 5★  (user rates driver)
    ("trip-1", "user-1", "USER",   "driver-A", "DRIVER", 5),
    // driver-A → user-1: 4★  (driver rates user)
    ("trip-1", "driver-A", "DRIVER", "user-1", "USER",   4),
    // user-2 → driver-A: 3★
    ("trip-2", "user-2", "USER",   "driver-A", "DRIVER", 3),
    // driver-A → user-2: 5★
    ("trip-2", "driver-A", "DRIVER", "user-2", "USER",   5),
    // user-3 → driver-B: 1★
    ("trip-3", "user-3", "USER",   "driver-B", "DRIVER", 1),
    // driver-B → user-3: 2★
    ("trip-3", "driver-B", "DRIVER", "user-3", "USER",   2)
  ).toDF("trip_id","from_id","from_role","to_id","to_role","stars")

  "driverRankings" should "compute avg stars received by driver" in {
    val result = RatingBatchJob.driverRankings(ratings).collect()
    // driver-A avg = (5+3)/2 = 4.0  driver-B avg = 1.0
    result.head.getAs[String]("driver_id")  shouldBe "driver-A"
    result.head.getAs[Double]("avg_stars")  shouldBe 4.0
    result.last.getAs[String]("driver_id")  shouldBe "driver-B"
    result.last.getAs[Double]("avg_stars")  shouldBe 1.0
  }

  it should "count only ratings where to_role=DRIVER" in {
    val result = RatingBatchJob.driverRankings(ratings)
    result.agg(org.apache.spark.sql.functions.sum("total_ratings"))
      .as[Long].first() shouldBe 3L
  }

  "userRankings" should "compute avg stars received by user" in {
    val result = RatingBatchJob.userRankings(ratings).collect()
    // user-1=4★  user-2=5★  user-3=2★  → top is user-2
    result.head.getAs[String]("user_id") shouldBe "user-2"
    result.head.getAs[Double]("avg_stars") shouldBe 5.0
  }

  "starDistribution" should "count ratings per star value" in {
    val dist = RatingBatchJob.starDistribution(ratings).collect()
      .map(r => r.getAs[Int]("stars") -> r.getAs[Long]("count")).toMap
    dist(1) shouldBe 1L
    dist(2) shouldBe 1L
    dist(3) shouldBe 1L
    dist(4) shouldBe 1L
    dist(5) shouldBe 2L
  }

  "globalSummary" should "compute totals correctly" in {
    val row = RatingBatchJob.globalSummary(ratings).head()
    row.getAs[Long]("total_ratings")     shouldBe 6L
    row.getAs[Long]("trips_rated")       shouldBe 3L
    row.getAs[Long]("unique_raters")     shouldBe 5L
  }
}
