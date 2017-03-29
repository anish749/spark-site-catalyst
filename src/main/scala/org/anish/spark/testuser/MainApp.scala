package org.anish.spark.testuser

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by anish on 28/03/17.
  */
object MainApp {
  // Spark 2.0.0
  //    def main(args: Array[String]) {
  //      val sparkSession = SparkSession.builder().master("local[3]").appName("SCdatasourceTest").getOrCreate()
  //
  //      val df = sparkSession
  //        .read
  //        .format("org.anish.spark.sitecatalyst")
  //        .load("data/rawExportRZ")
  //
  //      df.printSchema()
  //      df.show()
  //
  //      df.coalesce(1)
  //        .write
  //        .mode(SaveMode.Overwrite)
  //        .format("org.anish.spark.sitecatalyst")
  //        .save("outputTest/")
  //    }

  // Spark 1.6 and earlier
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("SCdatasourceTest")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val df: DataFrame = sqlContext
      .read
      .format("org.anish.spark.sitecatalyst")
      .load("data/rawExportRZ")


    df.printSchema()
    df.show()

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("org.anish.spark.sitecatalyst")
      .save("outputTest/")
  }
}
