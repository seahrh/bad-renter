package com.sgcharts

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

package object badrenter {

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

}
