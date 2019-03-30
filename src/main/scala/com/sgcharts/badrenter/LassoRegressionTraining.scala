package com.sgcharts.badrenter

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LassoRegressionTraining extends Log4jLogging {
  private val APP_NAME: String = getClass.getName

  private[badrenter] final case class Params(
                                              srcDb: String = "",
                                              srcTable: String = "",
                                              testSetFirstId: Int = 0,
                                              modelPath: String = ""
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
      opt[String]("model_path").action((x, c) =>
        c.copy(srcTable = x)
      ).text("model s3 location")
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
         |select default_amount label
         |,name
         |,age
         |,house_id
         |,house_zip
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
        "house_zip"
      ))
      .setOutputCols(Array(
        "name_1hot",
        "house_id_1hot",
        "house_zip_1hot"
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
        "age_buckets",
        "rent_amount"
      ))
      .setOutputCol("features")
  }

  private def estimator(): LinearRegression = {
    new LinearRegression()
      .setElasticNetParam(1) // L1 regularization Lasso
      .setMaxIter(3)
      .setFeaturesCol("features")
  }

  private def validator(): CrossValidator = {
    val ni = nameIndex()
    val oh = oneHotEncoder()
    val ab = ageBuckets()
    val fs = features()
    val lr = estimator()
    val pipe = new Pipeline().setStages(Array(ni, oh, ab, fs, lr))
    val grid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(1, 0.1))
      .build()
    val eval = new RegressionEvaluator()
      .setMetricName("r2")
    val seed: Long = 11
    new CrossValidator()
      .setEstimator(pipe)
      .setEvaluator(eval)
      .setEstimatorParamMaps(grid)
      .setNumFolds(3)
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel
      .setSeed(seed)
  }

  private def timeSuffix: String = {
    LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
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
      val v: CrossValidator = validator()
      val vm: CrossValidatorModel = v.fit(train)
      val bestParams: ParamMap = bestEstimatorParamMap(vm)
      log.info(
        s"""
           |bestParams=$bestParams
           |getEstimatorParamMaps=${vm.getEstimatorParamMaps.toString}
           |avgMetrics=${vm.avgMetrics.toString}
           |explainParams=${vm.explainParams.toString}
           |CrossValidatorModel=${vm.toString}
         """.stripMargin)
      vm.save(s"${params.modelPath}_$timeSuffix")
    } finally {
      spark.close()
    }
  }

}
