package org.anish.spark.sitecatalyst

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Created by anish on 28/03/17.
  */
class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String])
  : BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    parameters.getOrElse("path", sys.error("'path' must be specified for our data."))
    new SiteCatalystRelation(parameters("path"), schema)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {


    data.write.options(parameters).format("com.databricks.spark.csv").mode(mode).save(parameters("path"))
    createRelation(sqlContext, parameters, data.schema)
  }

}