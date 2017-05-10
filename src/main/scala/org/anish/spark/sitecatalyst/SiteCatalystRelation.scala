package org.anish.spark.sitecatalyst

import java.io._

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by anish on 28/03/17.
  */
class SiteCatalystRelation(location: String, userSchema: StructType)
                          (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan with Serializable {

  private final val fileSep = File.separator

  override def schema: StructType = {
    if (this.userSchema != null) {
      this.userSchema
    }
    else {
      inferSchemaFromLookUpFile(getRandomLookUpFilePath)
    }
  }

  def getPairRdd = sqlContext
    .sparkContext
    .wholeTextFiles(location)
    .map(identity) // This creates a copy. Data gets cached later. This is needed as it internally uses NewHadoopRDDs

  def getRandomLookUpFilePath: String = {
    getPairRdd
      .filter(_._1 endsWith "lookup_data.tar.gz") // Filter file name extension
      .map(_._1) // Take only names
      .take(1)
      .toList
      .headOption
      .getOrElse(throw new FileNotFoundException("Look Up Data File not found"))
  }

  override def buildScan(): RDD[Row] = {
    val pairRdd = getPairRdd.cache

    // Reading Site Catalyst export
    // Step 1: Parse the Manifest File
    val manifestFilesPairRdd = pairRdd.filter(_._1 endsWith ".txt")
    val lookUpAndDataFilePairs = manifestFilesPairRdd.map(manifestPair => {
      val filePath = manifestPair._1
      val folder = filePath.split(fileSep).reverse.tail.reverse.mkString(fileSep)
      val fileName = filePath.split(fileSep).last

      val lookupFilePath = manifestPair._2
        .split("\n")
        .filter(_ startsWith "Lookup-File:")
        .map(_.split(":").last.trim)
        .map(x => folder + fileSep + x)
        .head // Assumption. There will always be only one look up file

      val dataFilePaths = manifestPair._2
        .split("\n")
        .filter(_ startsWith "Data-File:")
        .map(_.split(":").last.trim)
        .map(x => folder + fileSep + x)

      // TODO Implement MD5 Digest, by calculating the MD5 and check if it is present in the manifest file.
      (lookupFilePath, dataFilePaths)
    })
    val pairsToRead = lookUpAndDataFilePairs.collect.toList

    // Step 2: List look up files that are present on disk
    val lookUpFileList = pairRdd
      .filter(_._1 endsWith "lookup_data.tar.gz") // Filter file name extension
      .map(_._1) // Take only names
      .collect
      .toList

    // Step 3: List data files that are present on disk
    val dataFileList = pairRdd
      .filter(_._1 endsWith ".tsv.gz") // Filter file name extension
      .map(_._1) // Take only names
      .collect
      .toList

    // Read
    val ret = pairsToRead
      .map(x => {
        val (lookupFilePath, dataFilePaths) = x
        if (!(lookUpFileList contains lookupFilePath)) {
          throw new RuntimeException(s"Could not find $lookupFilePath")
        }

        // check if all the data files are present in data file list
        val missingDataFiles = dataFilePaths.filterNot(dataFilePath => dataFileList contains dataFilePath)
        if (missingDataFiles.nonEmpty) {
          throw new RuntimeException(s"Could not find $missingDataFiles")
        }

        dataFilePaths
          .map(sqlContext.read
            .option("delimiter", "\t")
            .option("header", "false")
            .format("com.databricks.spark.csv")
            // .csv(_)) // In Spark 2.0.0
            .load(_))
          .reduce(_.unionAll(_))
      }).reduce(_.unionAll(_))
      .rdd

    ret
  }

  def inferSchemaFromLookUpFile(lookUpFilePath: String): StructType = {
    val lkFile = sqlContext.sparkContext
      .wholeTextFiles(lookUpFilePath)
      .map(_._2)
      .collect
      .toList
      .head
      .toString

    val lkFileTarArchiveInputStream = new TarArchiveInputStream(new ByteArrayInputStream(lkFile.getBytes))
    var currentEntry = lkFileTarArchiveInputStream.getNextTarEntry
    // Skip other entries
    while (currentEntry != null && currentEntry.getName != "column_headers.tsv") {
      currentEntry = lkFileTarArchiveInputStream.getNextTarEntry
    }

    val br = new BufferedReader(new InputStreamReader(lkFileTarArchiveInputStream));
    val columnHeaderList = br.readLine.split("\t").toList

    br.close()
    lkFileTarArchiveInputStream.close()

    // Fields are assumed to be StringType
    val schemaFields = columnHeaderList.map { fieldName =>
      StructField(fieldName, StringType, nullable = true)
    }
    StructType(schemaFields)
  }


}
