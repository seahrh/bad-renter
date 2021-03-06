package com.sgcharts.badrenter

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LassoRegressionTraining extends Log4jLogging {
  private val APP_NAME: String = getClass.getName

  private[badrenter] final case class Params(
                                              srcDb: String = "",
                                              srcTable: String = "",
                                              modelPath: String = "",
                                              partition: String = "",
                                              sinkDb: String = "",
                                              sinkTable: String = "",
                                              sinkPath: String = ""
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
      opt[String]("model_path").action((x, c) =>
        c.copy(modelPath = x)
      ).text("model s3 location")
      opt[String]("partition").action((x, c) =>
        c.copy(partition = x)
      ).text("hive table partition specs")
      opt[String]("sink_db").action((x, c) =>
        c.copy(sinkDb = x)
      ).text("hive database")
      opt[String]("sink_table").action((x, c) =>
        c.copy(sinkTable = x)
      ).text("hive table")
      opt[String]("sink_path").action((x, c) =>
        c.copy(sinkPath = x)
      ).text("path where partition is stored")
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
         |where ${params.partition}
         |order by id
      """.stripMargin
    log.info(sql)
    spark.sql(sql)
  }

  private val nameIndex: StringIndexer = new StringIndexer()
    .setInputCol("name")
    .setOutputCol("name_index")
    .setHandleInvalid("keep")


  private val oneHotEncoder: OneHotEncoderEstimator = new OneHotEncoderEstimator()
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

  private val ageBuckets: Bucketizer = new Bucketizer()
    .setInputCol("age")
    .setOutputCol("age_buckets")
    .setSplits(Array(Double.NegativeInfinity, 0.0, 20.0, 30.0, 40.0, 50.0, 60.0, Double.PositiveInfinity))

  private val features: VectorAssembler = new VectorAssembler()
    .setInputCols(Array(
      "name_1hot",
      "house_id_1hot",
      "house_zip_1hot",
      "age_buckets",
      "rent_amount"
    ))
    .setOutputCol("features")

  private val estimator: LinearRegression = new LinearRegression()
    .setElasticNetParam(1) // L1 regularization Lasso
    .setMaxIter(3)
    .setFeaturesCol("features")

  private val validator: CrossValidator = {
    val pipe = new Pipeline().setStages(Array(
      nameIndex, oneHotEncoder, ageBuckets, features, estimator))
    val grid = new ParamGridBuilder()
      .addGrid(estimator.regParam, Array(1, 0.1))
      .build()
    val eval = new RegressionEvaluator()
      .setMetricName("r2")
    val seed: Long = 11
    new CrossValidator()
      .setEstimator(pipe)
      .setEvaluator(eval)
      .setEstimatorParamMaps(grid)
      .setNumFolds(3)
      .setParallelism(2) // Evaluate up to 2 parameter settings in parallel
      .setSeed(seed)
  }

  private def timeSuffix: String = {
    LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
  }

  private def saveResult(vm: CrossValidatorModel)
                        (implicit params: Params, spark: SparkSession): Unit = {
    val bestParams: ParamMap = bestEstimatorParamMap(vm)
    val res =
      s"""getEstimatorParamMaps=${vm.getEstimatorParamMaps.toString}
         |avgMetrics=${vm.avgMetrics mkString ", "}
         |explainParams=${vm.explainParams}
         |CrossValidatorModel=${vm.toString}
         """.stripMargin
    val modelPath = s"${params.modelPath}_$timeSuffix"
    vm.save(modelPath)
    val df: DataFrame = spark.createDataFrame(Seq(
      (modelPath, res, bestParams.toString)
    )).toDF("model", "result", "best_params")
    df.write.mode(SaveMode.Append).parquet(params.sinkPath)
  }

  def main(args: Array[String]): Unit = {
    implicit val params: Params = parse(args = args)
    log.info(params)
    implicit val spark: SparkSession = SparkSession.builder
      .appName(APP_NAME)
      .enableHiveSupport()
      .getOrCreate()
    try {
      val train: DataFrame = extract().coalesce(1).cache()
      val vm: CrossValidatorModel = validator.fit(train)
      saveResult(vm)
    } finally {
      spark.close()
    }
  }

}
