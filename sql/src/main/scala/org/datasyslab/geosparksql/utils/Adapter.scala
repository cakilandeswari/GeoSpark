/*
 * FILE: Adapter.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geosparksql.utils


import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.geosparksql.GeometryWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import org.datasyslab.geospark.enums.{FileDataSplitter, GeometryType}
import org.datasyslab.geospark.formatMapper.FormatMapper

object Shape  extends Enumeration {
  type Format = Value
  val GEOMETRY,WKT,WKB,GEOJSON = Value
}

object Adapter {

  private def toJavaRdd(dataFrame: DataFrame, geometryColId: Int): JavaRDD[Geometry] = {
    toRdd(dataFrame, geometryColId, Shape.GEOMETRY).toJavaRDD()
  }

  private def toJavaRdd(dataFrame: DataFrame): JavaRDD[Geometry] = {
    toRdd(dataFrame, 0, Shape.GEOMETRY).toJavaRDD()
  }

  private def toRdd(dataFrame: DataFrame): RDD[Geometry] = {
    dataFrame.rdd.map[Geometry](f =>
    {
      var geometry = f.get(0).asInstanceOf[Geometry]
      var fieldSize = f.size
      var userData = ""
      if (fieldSize > 1) {
        // Add all attributes into geometry user data
        for (i<- 1 until f.size) userData += f.get(i)+"\t"
        userData = userData.dropRight(1)
      }
      geometry.setUserData(userData)
      geometry
    })
  }

  private def toRdd(dataFrame: DataFrame, geometryColId: Int, shapeFormat: Shape.Format): RDD[Geometry] = {
    dataFrame.rdd.map[Geometry](f =>
    {
      val geometry:Geometry = shapeFormat match {
        case Shape.GEOMETRY => {
          f.get(geometryColId).asInstanceOf[Geometry]
        }
        case Shape.WKT => {
          val formatMapper = new FormatMapper(FileDataSplitter.WKT, false)
          formatMapper.readGeometry(f.get(geometryColId).asInstanceOf[String])
        }
        case Shape.WKB => {
          val formatMapper = new FormatMapper(FileDataSplitter.WKB, false)
          formatMapper.readGeometry(f.get(geometryColId).asInstanceOf[String])
        }
        case Shape.GEOJSON => {
          val formatMapper = new FormatMapper(FileDataSplitter.GEOJSON, false)
          formatMapper.readGeometry(f.get(geometryColId).asInstanceOf[String])
        }
      }

      val fieldSize = f.size
      var userData = ""
      if (fieldSize > 1) {
        // Add all attributes into geometry user data
        for (i<- 0 until geometryColId) userData += f.get(i)+"\t"
        for (i<- geometryColId + 1 until f.size) userData += f.get(i)+"\t"
        userData = userData.dropRight(1)
      }
      geometry.setUserData(userData)
      geometry
    })
  }

  private def toRdd(dataFrame: DataFrame, geometryFieldName:String, shapeFormat: Shape.Format): RDD[Geometry] = {
    val fieldList = dataFrame.schema.toList.map(f => f.name.toString)
    val geomColId = fieldList.indexOf(geometryFieldName)
    assert(geomColId >= 0)
    toRdd(dataFrame, geomColId, shapeFormat)
  }

  @deprecated( "use toSpatialRdd and append geometry column's name", "1.2" )
  def toSpatialRdd(dataFrame: DataFrame): SpatialRDD[Geometry] =
  {
    toSpatialRdd(dataFrame, "geometry", Shape.GEOMETRY)
  }

  /**
    * Convert a Spatial DF to a Spatial RDD. The geometry column can be at any place in the DF
    * @param dataFrame
    * @param geometryFieldName
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryFieldName:String): SpatialRDD[Geometry] =
  {
    toSpatialRdd(dataFrame, geometryFieldName, Shape.GEOMETRY )
  }

  /**
    * Convert a Spatial DF to a Spatial RDD. The geometry column can be at any place in the DF
    * @param dataFrame
    * @param geometryColId
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryColId: Int): SpatialRDD[Geometry] =
  {
    toSpatialRdd(dataFrame, geometryColId, Shape.GEOMETRY)
  }

  /**
    * Convert a Spatial DF to a Spatial RDD with a list of user-supplied col names (except geom col). The geometry column can be at any place in the DF.
    * @param dataFrame
    * @param geometryColId
    * @param fieldNames
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryColId: Int, fieldNames: List[String]): SpatialRDD[Geometry] =
  {
    toSpatialRdd(dataFrame, geometryColId, fieldNames, Shape.GEOMETRY)
  }

  /**
    * Convert a Spatial DF to a Spatial RDD with a list of user-supplied col names (except geom col). The geometry column can be at any place in the DF.
    * @param dataFrame
    * @param geometryFieldName
    * @param fieldNames
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryFieldName:String, fieldNames: List[String]): SpatialRDD[Geometry] = {
    toSpatialRdd(dataFrame, geometryFieldName,fieldNames, Shape.GEOMETRY )
  }

  /**
    * Convert a Spatial DF to a Spatial RDD. The geometry column can be at any place in the DF
    * @param dataFrame
    * @param geometryFieldName
    * @param shapeFormat
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryFieldName:String, shapeFormat: Shape.Format): SpatialRDD[Geometry] =
  {
    // Delete the field that have geometry
    if (dataFrame.schema.size==1) {
      toSpatialRdd(dataFrame, 0, List[String](),shapeFormat)
    }
    else {
      val fieldList = dataFrame.schema.toList.map(f => f.name.toString)
      toSpatialRdd(dataFrame, geometryFieldName, fieldList.filter(p => !p.equalsIgnoreCase(geometryFieldName)), shapeFormat)
    }
  }

  /**
    * Convert a Spatial DF to a Spatial RDD. The geometry column can be at any place in the DF
    * @param dataFrame
    * @param geometryColId
    * @param shapeFormat
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryColId: Int, shapeFormat: Shape.Format): SpatialRDD[Geometry] =
  {
    // Delete the field that have geometry
    if (dataFrame.schema.size==1) {
      toSpatialRdd(dataFrame, 0, List[String](),shapeFormat)
    }
    else {
      val fieldList = dataFrame.schema.toList.map(f => f.name.toString)
      val geometryFieldName = fieldList(geometryColId)
      toSpatialRdd(dataFrame, geometryColId, fieldList.filter(p => !p.equalsIgnoreCase(geometryFieldName)),shapeFormat)
    }
  }

  /**
    * Convert a Spatial DF to a Spatial RDD with a list of user-supplied col names (except geom col). The geometry column can be at any place in the DF.
    * @param dataFrame
    * @param geometryColId
    * @param fieldNames
    * @param shapeFormat
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryColId: Int, fieldNames: List[String], shapeFormat: Shape.Format): SpatialRDD[Geometry] =
  {
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = toRdd(dataFrame, geometryColId, shapeFormat).toJavaRDD()
    import scala.collection.JavaConversions._
    if (fieldNames.nonEmpty) spatialRDD.fieldNames = fieldNames
    else spatialRDD.fieldNames = null
    spatialRDD
  }

  /**
    * Convert a Spatial DF to a Spatial RDD with a list of user-supplied col names (except geom col). The geometry column can be at any place in the DF.
    * @param dataFrame
    * @param geometryFieldName
    * @param fieldNames
    * @param shapeFormat
    * @return
    */
  def toSpatialRdd(dataFrame: DataFrame, geometryFieldName:String, fieldNames: List[String], shapeFormat: Shape.Format): SpatialRDD[Geometry] =
  {
    val spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = toRdd(dataFrame, geometryFieldName, shapeFormat).toJavaRDD()
    import scala.collection.JavaConversions._
    if (fieldNames.nonEmpty) spatialRDD.fieldNames = fieldNames
    else spatialRDD.fieldNames = null
    spatialRDD
  }

  def toDf[T <:Geometry](spatialRDD: SpatialRDD[T], fieldNames: List[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialRDD.rawSpatialRDD.rdd.map[Row](f => Row.fromSeq(f.toString.split("\t",-1).toSeq))
    if (fieldNames!=null && fieldNames.nonEmpty)
    {
      val fieldArray = new Array[StructField](fieldNames.size+1)
      fieldArray(0) = StructField("geometry", StringType)
      for (i <- 1 until fieldArray.length) fieldArray(i) = StructField(fieldNames(i-1), StringType)
      val schema = StructType(fieldArray)
      sparkSession.createDataFrame(rowRdd, schema)
    }
    else {
      val fieldArray = new Array[StructField](rowRdd.take(1)(0).size)
      fieldArray(0) = StructField("geometry", StringType)
      for (i <- 1 until fieldArray.length) fieldArray(i) = StructField("_c" + i, StringType)
      val schema = StructType(fieldArray)
      sparkSession.createDataFrame(rowRdd, schema)
    }
  }

  def toDf[T <:Geometry](spatialRDD: SpatialRDD[T], sparkSession: SparkSession): DataFrame = {
    import scala.collection.JavaConverters._
    if (spatialRDD.fieldNames!=null) return toDf(spatialRDD, spatialRDD.fieldNames.asScala.toList, sparkSession)
    toDf(spatialRDD, null, sparkSession);
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialPairRDD.rdd.map[Row](f => {
      val seq1 = f._1.toString.split("\t").toSeq
      val seq2 = f._2.toString.split("\t").toSeq
      val result = seq1 ++ seq2
      Row.fromSeq(result)
    })
    val leftgeomlength = spatialPairRDD.rdd.take(1)(0)._1.toString.split("\t").length

    var fieldArray = new Array[StructField](rowRdd.take(1)(0).size)
    for (i <- fieldArray.indices) fieldArray(i) = StructField("_c" + i, StringType)
    fieldArray(0) = StructField("leftgeometry", StringType)
    fieldArray(leftgeomlength) = StructField("rightgeometry", StringType)
    val schema = StructType(fieldArray)
    sparkSession.createDataFrame(rowRdd, schema)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry], leftFieldnames: List[String], rightFieldNames: List[String], sparkSession: SparkSession): DataFrame = {
    val rowRdd = spatialPairRDD.rdd.map[Row](f => {
      val seq1 = f._1.toString.split("\t").toSeq
      val seq2 = f._2.toString.split("\t").toSeq
      val result = seq1 ++ seq2
      Row.fromSeq(result)
    })
    val leftgeometryName = List("leftgeometry")
    val rightgeometryName = List("rightgeometry")
    val fullFieldNames = leftgeometryName ++ leftFieldnames ++ rightgeometryName ++ rightFieldNames
    val schema = StructType(fullFieldNames.map(fieldName => StructField(fieldName, StringType)))
    sparkSession.createDataFrame(rowRdd, schema)
  }

  /*
   * Since UserDefinedType is hidden from users. We cannot directly return spatialRDD to spatialDf.
   * Let's wait for Spark side's change
   */
  /*
  def toSpatialDf(spatialRDD: SpatialRDD[Geometry], sparkSession: SparkSession): DataFrame =
  {
    val rowRdd = spatialRDD.rawSpatialRDD.rdd.map[Row](f =>
      {
        var seq = Seq(new GeometryWrapper(f))
        var otherFields = f.getUserData.asInstanceOf[String].split("\t").toSeq
        seq :+ otherFields
        Row.fromSeq(seq)
      }
      )
    var fieldArray = new Array[StructField](rowRdd.take(1)(0).size)
    fieldArray(0) = StructField("rddshape", ArrayType(ByteType, false))
    for (i <- 1 to fieldArray.length-1) fieldArray(i) = StructField("_c"+i, StringType)
    val schema = StructType(fieldArray)
    return sparkSession.createDataFrame(rowRdd, schema)
  }
  */
}