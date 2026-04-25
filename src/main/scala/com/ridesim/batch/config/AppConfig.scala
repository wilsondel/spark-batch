package com.ridesim.batch.config

final case class AppConfig(
    kafkaBrokers:    String,
    topicPayment:    String,
    topicRating:     String,
    outputDir:       String,
    outputFormat:    String,
    sparkMaster:     String,
    appName:         String,
    pgHost:          String,
    pgDb:            String,
    pgUser:          String,
    pgPassword:      String
) {
  def pgJdbcUrl: String = s"jdbc:postgresql://$pgHost:5432/$pgDb"
}

object AppConfig {
  def fromEnv(env: Map[String, String] = sys.env): AppConfig =
    AppConfig(
      kafkaBrokers = env.getOrElse("KAFKA_BROKERS",    "localhost:9092"),
      topicPayment = env.getOrElse("TOPIC_PAYMENT",    "topic.payment"),
      topicRating  = env.getOrElse("TOPIC_RATING",     "topic.rating"),
      outputDir    = env.getOrElse("OUTPUT_DIR",       "/opt/output"),
      outputFormat = env.getOrElse("OUTPUT_FORMAT",    "json"),
      sparkMaster  = env.getOrElse("SPARK_MASTER",     "local[*]"),
      appName      = env.getOrElse("APP_NAME",         "rides-batch"),
      pgHost       = env.getOrElse("POSTGRES_HOST",    "localhost"),
      pgDb         = env.getOrElse("POSTGRES_DB",      "ridesim"),
      pgUser       = env.getOrElse("POSTGRES_USER",    "postgres"),
      pgPassword   = env.getOrElse("POSTGRES_PASSWORD","postgres")
    )
}
