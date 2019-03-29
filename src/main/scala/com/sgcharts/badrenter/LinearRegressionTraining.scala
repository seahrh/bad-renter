package com.sgcharts.badrenter

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LinearRegressionTraining extends Log4jLogging {
  private val APP_NAME: String = getClass.getName

  private[badrenter] final case class Params(
                                              srcDb: String = "",
                                              srcTable: String = "",
                                              testSetFirstId: Int = 0
                                            )

  private def parse(args: Array[String]): Params = {
    val parser = new scopt.OptionParser[Params](APP_NAME) {
      head(APP_NAME)
      opt[String]("src_db").action((x, c) =>
        c.copy(srcDb = x)
      ).text("source database name")
      opt[String]("src_table").action((x, c) =>
        c.copy(srcTable = x)
      ).text("source table")
      opt[Int]("test_set_first_id").action((x, c) =>
        c.copy(testSetFirstId = x)
      ).text("First id that marks the beginning of test set")
      help("help").text("prints this usage text")
    }
    // Load parameters
    parser.parse(args, Params()) match {
      case Some(res) =>
        res
      case _ =>
        throw new IllegalStateException("fail to parse args")
    }
  }

  private def extract()(implicit params: Params, spark: SparkSession): DataFrame = {
    val sql: String =
      s"""
         |select default_amount
         |,name
         |,age
         |,house_id
         |,house_zip
         |,payment_date_year
         |,payment_date_month
         |,payment_date_day_of_week
         |,payment_date_day_of_month
         |,rent_amount
         |,id
         |from ${params.srcDb}.${params.srcTable}
         |where id<${params.testSetFirstId}
         |order by id
      """.stripMargin
    log.info(sql)
    spark.sql(sql)
  }

  private def nameIndex(): StringIndexer = {
    new StringIndexer()
      .setInputCol("name")
      .setOutputCol("name_index")
      .setHandleInvalid("keep")
  }

  private def oneHotEncoder(): OneHotEncoderEstimator = {
    new OneHotEncoderEstimator()
      .setInputCols(Array(
        "name_index",
        "house_id",
        "house_zip",
        "payment_date_year",
        "payment_date_month",
        "payment_date_day_of_week",
        "payment_date_day_of_month"
      ))
      .setOutputCols(Array(
        "name_1hot",
        "house_id_1hot",
        "house_zip_1hot",
        "payment_date_year_1hot",
        "payment_date_month_1hot",
        "payment_date_day_of_week_1hot",
        "payment_date_day_of_month_1hot"
      ))
      .setHandleInvalid("keep")
  }

  private def ageBuckets(): Bucketizer = {
    new Bucketizer()
      .setInputCol("age")
      .setOutputCol("age_buckets")
      .setSplits(Array(Double.NegativeInfinity, 0.0, 20.0, 30.0, 40.0, 50.0, 60.0, Double.PositiveInfinity))
  }

  private def features(): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(Array(
        "name_1hot",
        "house_id_1hot",
        "house_zip_1hot",
        "payment_date_year_1hot",
        "payment_date_month_1hot",
        "payment_date_day_of_week_1hot",
        "payment_date_day_of_month_1hot",
        "age_buckets",
        "rent_amount"
      ))
      .setOutputCol("features")
  }

  private def linearRegression(): LinearRegression = {
    new LinearRegression()
      .setMaxIter(3)
      .setLabelCol("default_amount")
      .setFeaturesCol("features")
  }

  private def linearRegressionValidator(): CrossValidator = {
    val ni = nameIndex()
    val oh = oneHotEncoder()
    val ab = ageBuckets()
    val fs = features()
    val lr = linearRegression()
    val pipe = new Pipeline().setStages(Array(ni, oh, ab, fs, lr))
    val grid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()
    val eval = new RegressionEvaluator()
      .setMetricName("r2")
      .setLabelCol("default_amount")
    val seed: Long = 11
    new CrossValidator()
      .setEstimator(pipe)
      .setEvaluator(eval)
      .setEstimatorParamMaps(grid)
      .setNumFolds(3)
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel
      .setSeed(seed)
  }

  def main(args: Array[String]): Unit = {
    implicit val params: Params = parse(args = args)
    log.info(params)
    implicit val spark: SparkSession = SparkSession.builder
      .appName(APP_NAME)
      .enableHiveSupport()
      .getOrCreate()
    try {
      val train: DataFrame = extract()
      val lrv: CrossValidator = linearRegressionValidator()
      val lrm: CrossValidatorModel = lrv.fit(train)
      log.info(
        s"""
           |getEstimatorParamMaps=${lrm.getEstimatorParamMaps}
           |avgMetrics=${lrm.avgMetrics}
           |explainParams=${lrm.explainParams}
         """.stripMargin)
      lrm.save("s3://com.sgcharts.ap-southeast-1/models/linear_regression_r2")
    } finally {
      spark.close()
    }
  }

}
