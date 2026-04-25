package com.ridesim.batch.writer

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object ResultWriter {

  private val log = LoggerFactory.getLogger(getClass)

  def write(df: DataFrame, outputDir: String, name: String, format: String): Unit = {
    val path = s"$outputDir/$name"
    log.info(s"writing_result name=$name path=$path format=$format")
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .format(format)
      .save(path)
    log.info(s"result_saved name=$name")
  }

  def writePg(df: DataFrame, jdbcUrl: String, user: String, password: String, table: String): Unit = {
    log.info(s"writing_to_postgres table=$table url=$jdbcUrl")
    df.write
      .format("jdbc")
      .option("url",      jdbcUrl)
      .option("dbtable",  table)
      .option("user",     user)
      .option("password", password)
      .option("driver",   "org.postgresql.Driver")
      .mode("overwrite")
      .save()
    log.info(s"postgres_write_done table=$table")
  }

  def show(df: DataFrame, name: String, rows: Int = 20): Unit = {
    println(s"\n========== $name ==========")
    df.show(rows, truncate = false)
  }
}
