package com.ridesim.batch

import com.ridesim.batch.config.AppConfig
import com.ridesim.batch.jobs.{PaymentBatchJob, RatingBatchJob}
import com.ridesim.batch.writer.ResultWriter
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.fromEnv()
    log.info(s"batch_job_started config=$cfg")

    val spark = SparkSession.builder()
      .appName(cfg.appName)
      .master(cfg.sparkMaster)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.session.timeZone",   "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    runPaymentJob(spark, cfg)
    runRatingJob(spark, cfg)

    spark.stop()
    log.info("batch_job_completed")
  }

  private def runPaymentJob(spark: SparkSession, cfg: AppConfig): Unit = {
    log.info("payment_job_started")
    val payments = PaymentBatchJob.readPayments(spark, cfg.kafkaBrokers, cfg.topicPayment)
    payments.cache()

    val total = payments.count()
    log.info(s"payment_records_read count=$total")

    if (total == 0) {
      log.warn("payment_job_no_data_skipping")
      return
    }

    val summary    = PaymentBatchJob.globalSummary(payments)
    val byMethod   = PaymentBatchJob.revenueByMethod(payments)
    val byDriver   = PaymentBatchJob.revenueByDriver(payments)
    val byUser     = PaymentBatchJob.spendingByUser(payments)

    ResultWriter.show(summary,  "PAYMENT GLOBAL SUMMARY")
    ResultWriter.show(byMethod, "REVENUE BY PAYMENT METHOD")
    ResultWriter.show(byDriver, "TOP DRIVERS BY EARNINGS",   rows = 10)
    ResultWriter.show(byUser,   "TOP USERS BY SPENDING",     rows = 10)

    ResultWriter.write(summary,  cfg.outputDir, "payment_summary",     cfg.outputFormat)
    ResultWriter.write(byMethod, cfg.outputDir, "payment_by_method",   cfg.outputFormat)
    ResultWriter.write(byDriver, cfg.outputDir, "payment_by_driver",   cfg.outputFormat)
    ResultWriter.write(byUser,   cfg.outputDir, "payment_by_user",     cfg.outputFormat)

    payments.unpersist()
    log.info("payment_job_completed")
  }

  private def runRatingJob(spark: SparkSession, cfg: AppConfig): Unit = {
    log.info("rating_job_started")
    val ratings = RatingBatchJob.readRatings(spark, cfg.kafkaBrokers, cfg.topicRating)
    ratings.cache()

    val total = ratings.count()
    log.info(s"rating_records_read count=$total")

    if (total == 0) {
      log.warn("rating_job_no_data_skipping")
      return
    }

    val summary     = RatingBatchJob.globalSummary(ratings)
    val drivers     = RatingBatchJob.driverRankings(ratings)
    val users       = RatingBatchJob.userRankings(ratings)
    val starDist    = RatingBatchJob.starDistribution(ratings)

    ResultWriter.show(summary,  "RATING GLOBAL SUMMARY")
    ResultWriter.show(starDist, "STAR DISTRIBUTION")
    ResultWriter.show(drivers,  "TOP DRIVERS BY RATING",  rows = 10)
    ResultWriter.show(users,    "TOP USERS BY RATING",    rows = 10)

    ResultWriter.write(summary,  cfg.outputDir, "rating_summary",        cfg.outputFormat)
    ResultWriter.write(drivers,  cfg.outputDir, "rating_driver_ranking", cfg.outputFormat)
    ResultWriter.write(users,    cfg.outputDir, "rating_user_ranking",   cfg.outputFormat)
    ResultWriter.write(starDist, cfg.outputDir, "rating_star_dist",      cfg.outputFormat)

    ratings.unpersist()
    log.info("rating_job_completed")
  }
}
