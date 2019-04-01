package com.sgcharts.badrenter

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.scalatest.FlatSpec

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SmoteSpec extends FlatSpec with DataFrameSuiteBase {
  override implicit def reuseContextIfPossible: Boolean = true

  "Child sample" should "take either parent's value in a discrete String attribute" in {
    val colName = "col"
    val s = Smote(
      sample = spark.emptyDataFrame,
      discreteStringAttributes = Seq[String](colName),
      discreteLongAttributes = Seq.empty[String],
      continuousAttributes = Seq.empty[String],
      bucketLength = 1
    )(spark)
    val schema = StructType(Seq(StructField(colName,StringType)))
    var leftCount: Int = 0
    var rightCount: Int = 0
    for (_ <- 0 until 100) {
      val left = new GenericRowWithSchema(Array("left"), schema)
      val right = new GenericRowWithSchema(Array("right"), schema)
      val child = s.child(left, right)
      child.getAs[String](colName) match {
        case "left" => leftCount += 1
        case _ => rightCount += 1
      }
    }
    assert(leftCount > 0 && rightCount > 0)
  }

  it should "take either parent's value in a discrete Long attribute" in {
    val colName = "col"
    val s = Smote(
      sample = spark.emptyDataFrame,
      discreteStringAttributes = Seq.empty[String],
      discreteLongAttributes = Seq[String](colName),
      continuousAttributes = Seq.empty[String],
      bucketLength = 1
    )(spark)
    val schema = StructType(Seq(StructField(colName,LongType)))
    var leftCount: Int = 0
    var rightCount: Int = 0
    for (_ <- 0 until 100) {
      val left = new GenericRowWithSchema(Array(1L), schema)
      val right = new GenericRowWithSchema(Array(2L), schema)
      val child = s.child(left, right)
      child.getAs[Long](colName) match {
        case 1 => leftCount += 1
        case _ => rightCount += 1
      }
    }
    assert(leftCount > 0 && rightCount > 0)
  }

  it should "take a value between its parents in a continuous attribute" in {
    val colName = "col"
    val s = Smote(
      sample = spark.emptyDataFrame,
      discreteStringAttributes = Seq.empty[String],
      discreteLongAttributes = Seq.empty[String],
      continuousAttributes = Seq[String](colName),
      bucketLength = 1
    )(spark)
    val schema = StructType(Seq(StructField(colName,DoubleType)))
    for (_ <- 0 until 100) {
      val left = new GenericRowWithSchema(Array(1D), schema)
      val right = new GenericRowWithSchema(Array(4D), schema)
      val child = s.child(left, right)
      val a = child.getAs[Double](colName)
      assert(a >= 1D && a <= 4D)
    }
  }

}
