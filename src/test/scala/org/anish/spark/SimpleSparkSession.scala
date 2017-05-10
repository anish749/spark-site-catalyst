package org.anish.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A lazy evaluated Spark Session for managing Spark Context and SQL Context for Spark 1.6 or less.
  *
  * Created by anish on 25/04/17.
  */
object SimpleSparkSession {

  // Lazy val is used to prevent multiple spark context creations
  lazy val sparkContext: SparkContext = {
    val sparkConf = new SparkConf()
      .setMaster("local[12]")
      .setAppName("Spark-Unit-Tests")
      .set("spark.driver.host", "localhost")
      .set("spark.scheduler.mode", "FAIR") // Because multiple tests can run in different threads
    new SparkContext(sparkConf)
  }

  lazy val sqlContext: SQLContext = {
    new SQLContext(sparkContext)
  }
}
