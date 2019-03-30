package com.sgcharts.badrenter

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}

object ModelTesting extends Log4jLogging {
  private val APP_NAME: String = getClass.getName

  private[badrenter] final case class Params(
                                              srcDb: String = "",
                                              srcTable: String = "",
                                              testSetFirstId: Int = 0,
                                              modelPath: String = "",
                                              partition: String = ""
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
        c.copy(modelPath = x)
      ).text("model s3 location")
      opt[String]("partition").action((x, c) =>
        c.copy(partition = x)
      ).text("hive table partition specs")
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
         |and id>=${params.testSetFirstId}
         |order by id
      """.stripMargin
    log.info(sql)
    spark.sql(sql)
  }

  private def test(data: DataFrame)(implicit params: Params): Unit = {
    val model: CrossValidatorModel = CrossValidatorModel.load(params.modelPath)
    val transformed: DataFrame = model.transform(data)
    transformed.printSchema()
    val res = transformed.rdd.map { row =>
        val pred = row.getAs[Double]("prediction")
        val label = row.getAs[Int]("label").toDouble
        (pred, label)
    }
    val metrics = new RegressionMetrics(res)
    log.info(s"r2=${metrics.r2}, MAE=${metrics.meanAbsoluteError}")
  }

  def main(args: Array[String]): Unit = {
    implicit val params: Params = parse(args = args)
    log.info(params)
    implicit val spark: SparkSession = SparkSession.builder
      .appName(APP_NAME)
      .enableHiveSupport()
      .getOrCreate()
    try {
      val data: DataFrame = extract()
      test(data)
    } finally {
      spark.close()
    }
  }

}
