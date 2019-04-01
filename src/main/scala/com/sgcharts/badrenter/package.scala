package com.sgcharts

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

package object badrenter extends Log4jLogging {

  /**
    * Based on https://stackoverflow.com/a/34667785/519951
    * @param cvModel CrossValidatorModel
    * @return best values for parameters
    */
  def bestEstimatorParamMap(cvModel: CrossValidatorModel): ParamMap = {
    val arr: Array[(ParamMap, Double)] = cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)
    if (cvModel.getEvaluator.isLargerBetter) {
      arr.maxBy(_._2)._1
    } else {
      arr.minBy(_._2)._1
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def update(origin: Row, fieldName: String, value: Any): Row = {
    val i: Int = origin.fieldIndex(fieldName)
    val ovs = origin.toSeq
    var vs = ovs.slice(0, i) ++ Seq(value)
    val size: Int = ovs.size
    if (i != size - 1) {
      vs ++= ovs.slice(i + 1, size)
    }
    new GenericRowWithSchema(vs.toArray, origin.schema)
  }

  // based on https://stackoverflow.com/a/40801637/519951
  def toDF(rows: Array[Row], schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(rows.toList.asJava, schema)
  }

  private def union(left: DataFrame, right: DataFrame): DataFrame = {
    val cols: Array[String] = left.columns
    val res: DataFrame = left.union(right.select(cols.head, cols.tail: _*))
    log.debug(
      s"""
         |Left schema ${left.schema.treeString}
         |Right schema ${right.schema.treeString}
         |Union schema ${res.schema.treeString}
       """.stripMargin)
    res
  }

  /**
    * DataFrame workaround for Dataset union bug.
    * Union is performed in order of the operands.
    * @see [[https://issues.apache.org/jira/browse/SPARK-21109]]
    * @param head first DataFrame
    * @param tail varargs of successive DataFrames
    * @return new DataFrame representing the union
    */
  def union(head: DataFrame, tail: DataFrame*): DataFrame = {
    val dfs: List[DataFrame] = head :: tail.toList
    dfs.reduceLeft(union)
  }

}
