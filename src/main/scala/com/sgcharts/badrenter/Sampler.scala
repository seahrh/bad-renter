package com.sgcharts.badrenter

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import com.sgcharts.sparkutil.Smote

object Sampler extends Log4jLogging {
  private val APP_NAME: String = getClass.getName

  private[badrenter] final case class Params(
                                              srcDb: String = "",
                                              srcTable: String = "",
                                              testSetFirstId: Int = 0,
                                              partition: String = "",
                                              sinkDb: String = "",
                                              sinkTable: String = "",
                                              sinkPath: String = "",
                                              smoteBucketLength: Int = 1,
                                              smoteK: Int = 1
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
      opt[Int]("smote_bucket_length").action((x, c) =>
        c.copy(smoteBucketLength = x)
      ).text("smote bucket length")
      opt[Int]("smote_k").action((x, c) =>
        c.copy(smoteK = x)
      ).text("smote k nearest neighbours")
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

  private def extract()(implicit params: Params, spark: SparkSession): Dataset[Payment] = {
    val sql: String =
      s"""
         |select id
         |,name
         |,age
         |,house_id
         |,house_zip
         |,rent_amount
         |,default_amount
         |,false is_syn
         |from ${params.srcDb}.${params.srcTable}
         |where ${params.partition}
         |and id<${params.testSetFirstId}
         |order by id
      """.stripMargin
    log.info(sql)
    spark.sql(sql).as[Payment](Encoders.product[Payment])
  }

  private def transform(it: Iterator[Row]): Iterator[Payment] = {
    it.map(row => {
      Payment.from(row)
    })
  }

  private def load(
                    originalSample: Dataset[Payment],
                    syntheticSample: Dataset[Payment]
                  )(implicit params: Params, spark: SparkSession): Unit = {
    val ds: Dataset[Payment] = union(originalSample.toDF, syntheticSample.toDF)
      .selectExpr(
        "cast(ROW_NUMBER() OVER (ORDER BY rand()) as bigint) id",
        "name",
        "age",
        "house_id",
        "house_zip",
        "rent_amount",
        "default_amount",
        "is_syn"
      ).as[Payment](Encoders.product[Payment])
    ParquetTablePartition[Payment](
      ds = ds,
      db = params.sinkDb,
      table = params.sinkTable,
      path = params.sinkPath,
      partition = params.partition
    ).overwrite()
  }

  def main(args: Array[String]): Unit = {
    implicit val params: Params = parse(args = args)
    log.info(params)
    implicit val spark: SparkSession = SparkSession.builder
      .appName(APP_NAME)
      .enableHiveSupport()
      .getOrCreate()
    try {
      val data: Dataset[Payment] = extract()
      val maj = data.filter(_.default_amount == 0)
      val majSize: Long = maj.count()
      val min = data.filter(_.default_amount != 0)
      val minSize: Long = min.count()
      val sizeMultiplier: Int = Math.max((majSize / minSize).toInt, 2)
      log.info(s"sizeMultiplier=$sizeMultiplier")
      val sample: DataFrame = min.toDF.selectExpr(
        "name",
        "cast(house_id as bigint) as house_id",
        "cast(house_zip as bigint) as house_zip",
        "cast(age as double) as age",
        "cast(rent_amount as double) as rent_amount",
        "cast(default_amount as double) as default_amount"
      )
      val synDf: DataFrame = Smote(
        sample = sample,
        discreteStringAttributes = Seq("name"),
        discreteLongAttributes = Seq("house_id", "house_zip"),
        continuousAttributes = Seq("age", "rent_amount", "default_amount"),
        numHashTables = 2,
        sizeMultiplier = sizeMultiplier,
        numNearestNeighbours = params.smoteK
      ).syntheticSample
      val syn: Dataset[Payment] = synDf.mapPartitions(transform)(Encoders.product[Payment])
      load(data, syn)
    } finally {
      spark.close()
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
                                    rent_amount: Int,
                                    default_amount: Int,
                                    is_syn: Boolean
                                  )

  private object Payment {
    private[badrenter] def from(row: Row): Payment = {
      val name = row.getAs[String]("name")
      val age = row.getAs[Double]("age").toInt
      val house_id = row.getAs[Long]("house_id").toInt
      val house_zip = row.getAs[Long]("house_zip").toInt
      val rent_amount = row.getAs[Double]("rent_amount").toInt
      val default_amount = row.getAs[Double]("default_amount").toInt
      Payment(
        id = 0,
        name = name,
        age = age,
        house_id = house_id,
        house_zip = house_zip,
        rent_amount = rent_amount,
        default_amount = default_amount,
        is_syn = true
      )
    }
  }

}
