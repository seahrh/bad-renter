package com.sgcharts

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidatorModel

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

}
