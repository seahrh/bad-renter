package com.sgcharts.badrenter

import org.apache.spark.sql.{DataFrame, SparkSession}

object Cleaner extends Log4jLogging {
  private val APP_NAME: String = getClass.getName

  def main(args: Array[String]): Unit = {
    implicit val params: Params = parse(args = args)
    implicit val spark: SparkSession = SparkSession.builder
      .appName(APP_NAME)
      .enableHiveSupport()
      .getOrCreate()
    try {
      val df: DataFrame = extract()
      df.explain()
      log.info(s"count=${df.count}")
    } finally {
      spark.close()
    }
  }

  private def extract()(implicit params: Params, spark: SparkSession): DataFrame = {
    val sql: String =
      s"""
         |select name
         |,dob
         |,house_id
         |,house_zip
         |,payment_date
         |,payment_amount
         |,rent_amount
         |from ${params.srcDb}.${params.srcTable}
         |order by rand()
      """.stripMargin
    log.info(sql)
    spark.sql(sql)
  }

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
}

private final case class Params(
                                 srcDb: String = "",
                                 srcTable: String = "",
                                 sinkDb: String = "",
                                 sinkTable: String = "",
                                 sinkPartition: String = "",
                                 sinkPath: String = ""
                               )
