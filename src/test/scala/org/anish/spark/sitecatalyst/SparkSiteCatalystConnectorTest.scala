package org.anish.spark.sitecatalyst

import org.anish.spark.{SimpleSparkSession, SparkTestUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by anish on 07/05/17.
  */
class SparkSiteCatalystConnectorTest extends FlatSpec with Matchers {
  val siteCatalystSampleExportPath = "data/rawExportRZ"

  behavior of "SparkSiteCatalystConnectorTest"

  it should "read site catalyst data correctly from a given source and schema from a look up data file" in {
    val sqlContext = SimpleSparkSession.sqlContext

    val df: DataFrame = sqlContext
      .read
      .format("org.anish.spark.sitecatalyst")
      .load(siteCatalystSampleExportPath)

    df.cache

    df.count shouldBe 592
    df.columns.length shouldBe 294

  }

  // While writing to disk, it internally uses spark csv package from data bricks
  it should "successfully write as csv to disk for storage" in {
    val sqlContext = SimpleSparkSession.sqlContext

    val df: DataFrame = sqlContext
      .read
      .format("org.anish.spark.sitecatalyst")
      .load(siteCatalystSampleExportPath)

    val tmpOutputPath = s"/tmp/spark-site-catalyst-test_${System.currentTimeMillis}"

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("org.anish.spark.sitecatalyst")
      .save(tmpOutputPath)

    val actualDf = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .load(tmpOutputPath)

    val expectedDf = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .load(SparkTestUtils.getResourcePath("/expectedOutputs/sitecatalystcsv"))

    try {
      SparkTestUtils.dfEquals(actualDf, expectedDf)
    }
    finally {
      // Clear the tmp path where the HashSet Index was stored
      val filesystemPath = new Path(tmpOutputPath)
      val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      if (fs.exists(filesystemPath))
        fs.delete(filesystemPath, true)
    }
  }
}
