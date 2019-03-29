package com.sgcharts.badrenter

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.sgcharts.badrenter.LinearRegressionTraining.Params
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}

object LinearRegressionTraining extends Log4jLogging {
  private val APP_NAME: String = getClass.getName

  private[badrenter] final case class Params(
                                              srcDb: String = "",
                                              srcTable: String = "",
                                              sinkDb: String = "",
                                              sinkTable: String = "",
                                              sinkPartition: String = "",
                                              sinkPath: String = "",
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
      opt[String]("sink_db").action((x, c) =>
        c.copy(sinkDb = x)
      ).text("hive database")
      opt[String]("sink_table").action((x, c) =>
        c.copy(sinkTable = x)
      ).text("hive table")
      opt[String]("sink_partition").action((x, c) =>
        c.copy(sinkPartition = x)
      ).text("hive partition")
      opt[String]("sink_path").action((x, c) =>
        c.copy(sinkPath = x)
      ).text("path where partition is stored")
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

  private def pipeline()(implicit params: Params): Pipeline = {
    val ageBuckets = new Bucketizer()
      .setInputCol("age")
      .setOutputCol("age_buckets")
      .setSplits(Array(Double.NegativeInfinity, 0.0, 20.0, 30.0, 40.0, 50.0, 60.0, Double.PositiveInfinity))
    val lr = new LinearRegression()
      .setMaxIter(3)
      .setRegParam(0.001)
    new Pipeline().setStages(Array(ageBuckets, lr))
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
      val pipe: Pipeline = pipeline()
      val model: PipelineModel = pipe.fit(train)
    } finally {
      spark.close()
    }
  }

}
