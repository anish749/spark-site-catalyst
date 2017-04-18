# Spark Site Catalyst

A smart data source for reading Adobe Site Catalyst data warehouse exports directly in Apache Spark (tested in Scala).

## Introduction
Apache Spark provides the data source API in which we can create our own custom relations which take care of the metadata available from a particular source.
This helps in directly creating a Data Frame out of any data format and any data source. 
Adobe Site Catalyst / Omniture is the tool for web and app based usage analytics as part of Adobe Analytics package. Data can be exported from Site Catalyst for development of custom algorithms and models. 
Data warehouse feeds from Adobe Site Catalyst consists of 3 files (among many other feed export configurations). One with the actual log of the hits recorded. One tar ball with reference look up files, and one manifest file containing metadata of the export.
This package essentially acts as a smart connector between files exported by Site Catalyst and a Spark data frame. It essentially takes away the pre processing effort involved in loading the data into from CSV files and converting into a Spark Data Frame manually.
This version of the package currently can only read formats where hit data comes separately and look up data comes separately. This internally uses Data Bricks Spark CSV module to parse the tsv files exported by Site Catalyst.

## Build
The project is written in Scala 2.11.6, and dependencies are managed using Maven 3.3.9
```
mvn clean install
```

## Usage

### Adding this package as a dependency
Once installed via maven, this package can be added as a dependency as follows:
```
<dependency>
    <groupId>org.anish.spark</groupId>
    <artifactId>spark-site-catalyst</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
Note: This is not available as a package in Maven Central repository.

### In Spark applications (Scala API)
Create a data frame from the folder containing all the exported files (Hit data, Look up data and Manifest files)
```
val df: DataFrame = sqlContext
  .read
  .format("org.anish.spark.sitecatalyst")
  .load("data/rawExportRZ") // give local or hdfs or s3n path here

df.printSchema() // Print schema to see if it was read correctly.
df.show() // Use the data for further processing.

```
A detailed usage of this package is described in the object org.anish.spark.example.MainApp with examples for Spark v2.0.0 and v1.5.2.

For testing the application I've included some data files from Randy Zwitch's blog at http://randyzwitch.com/ and are present the folder 'data/rawExportRZ'.
Reading the data in R is described in this blog post using the files linked here: http://randyzwitch.com/adobe-analytics-clickstream-raw-data-feed/
I've developed this application using the same files. Link for downloading the files are available in the blog post.
Files included courtesy of Randy Zwitch.
