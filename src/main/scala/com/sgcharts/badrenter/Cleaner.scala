package com.sgcharts.badrenter

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Period, ZoneOffset}
import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

object Cleaner extends Log4jLogging {
  private val APP_NAME: String = getClass.getName

  def main(args: Array[String]): Unit = {
    implicit val params: Params = parse(args = args)
    log.info(params)
    implicit val spark: SparkSession = SparkSession.builder
      .appName(APP_NAME)
      .enableHiveSupport()
      .getOrCreate()
    try {
      val df: DataFrame = extract()
      df.explain
      val ds: Dataset[Payment] = transform(df)
      ds.explain
      load(ds)
    } finally {
      spark.close()
    }
  }

  private def extract()(implicit params: Params, spark: SparkSession): DataFrame = {
    val sql: String =
      s"""
         |select trim(name)
         |,trim(dob)
         |,house_id
         |,house_zip
         |,trim(payment_date)
         |,payment_amount
         |,rent_amount
         |,ROW_NUMBER() OVER (ORDER BY rand()) AS id
         |from ${params.srcDb}.${params.srcTable}
      """.stripMargin
    log.info(sql)
    spark.sql(sql)
  }

  private def transform(it: Iterator[Row]): Iterator[Payment] = {
    it.map(row => {
      Payment.from(row)
    })
  }

  private def transform(df: DataFrame): Dataset[Payment] = {
    df.mapPartitions(transform)(Encoders.product[Payment])
  }

  private def load(ds: Dataset[Payment])(implicit params: Params, spark: SparkSession): Unit = {
    ParquetTablePartition[Payment](
      ds = ds,
      db = params.sinkDb,
      table = params.sinkTable,
      path = params.sinkPath,
      partition = params.sinkPartition
    ).overwrite()
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

// Schema of the sink table
@SerialVersionUID(1L)
private final case class Payment(
                                  id: Long,
                                  name: String,
                                  age: Int,
                                  house_id: Int,
                                  house_zip: Int,
                                  payment_date: Date,
                                  payment_date_year: Int,
                                  payment_date_month: Int,
                                  payment_date_day_of_week: Int,
                                  payment_date_day_of_month: Int,
                                  payment_amount: Int,
                                  rent_amount: Int,
                                  default_amount: Int
                                )

private object Payment {
  private val today: LocalDate = LocalDate.now(ZoneOffset.UTC)

  private[badrenter] def from(row: Row): Payment = {
    val name = row.getAs[String]("name")
    val dob_str = row.getAs[String]("dob")
    val house_id = row.getAs[Int]("house_id")
    val house_zip = row.getAs[Int]("house_zip")
    val payment_date_str = row.getAs[String]("payment_date")
    val opa: Option[Int] = Option(row.getAs[Int]("payment_amount"))
    val rent_amount = row.getAs[Int]("rent_amount")
    val id = row.getAs[Long]("id")
    val dob: LocalDate = toLocalDate(dob_str)
    val age: Int = Period.between(dob, today).getYears
    val payment_date_ld: LocalDate = toLocalDate(payment_date_str)
    val payment_date_year: Int = payment_date_ld.getYear
    val payment_date_month: Int = payment_date_ld.getMonthValue
    val payment_date_day_of_week: Int = payment_date_ld.getDayOfWeek.getValue
    val payment_date_day_of_month: Int = payment_date_ld.getDayOfMonth
    val payment_date: Date = toDate(payment_date_ld)
    val payment_amount: Int = paymentAmount(opa, rent_amount)
    val default_amount: Int = rent_amount - payment_amount
    Payment(
      id = id,
      name = name,
      age = age,
      house_id = house_id,
      house_zip = house_zip,
      payment_date = payment_date,
      payment_date_year = payment_date_year,
      payment_date_month = payment_date_month,
      payment_date_day_of_week = payment_date_day_of_week,
      payment_date_day_of_month = payment_date_day_of_month,
      payment_amount = payment_amount,
      rent_amount = rent_amount,
      default_amount = default_amount
    )
  }

  private def paymentAmount(opa: Option[Int], ra: Int): Int = opa match {
    case Some(pa) =>
      Math.min(Math.abs(pa), ra)
    case _ => 0
  }

  private def toDate(localDate: LocalDate): Date = {
    Date.valueOf(localDate)
  }

  private def toLocalDate(dateStr: String): LocalDate = {
    val pattern: String = if (dateStr contains "/") {
      "MM/dd/yyyy"
    } else {
      "yyyyMMdd"
    }
    LocalDate.parse(dateStr, DateTimeFormatter.ofPattern(pattern))
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
