package com.sgcharts.badrenter

import org.apache.spark.ml
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

final case class Smote(
                        sample: DataFrame,
                        discreteStringAttributes: Seq[String],
                        discreteLongAttributes: Seq[String],
                        continuousAttributes: Seq[String],
                        bucketLength: Double,
                        numHashTables: Int = 1,
                        sizeMultiplier: Int = 2,
                        numNearestNeighbours: Int = 4
                      )(implicit spark: SparkSession) extends Log4jLogging {
  private val rand = new scala.util.Random
  private val featuresCol: String = "com_sgcharts_smote_features"
  private val allAttributes: Seq[String] =
    discreteStringAttributes ++ discreteLongAttributes ++ continuousAttributes

  private val stringIndexerOutputCols: Seq[String] = discreteStringAttributes.map { s =>
    s + "_indexed"
  }

  private val oneHotEncoderInputCols: Seq[String] = stringIndexerOutputCols ++ discreteLongAttributes

  private val oneHotEncoderOutputCols: Seq[String] =
    oneHotEncoderInputCols.map { s =>
      s + "_1hot"
    }

  private val assemblerInputCols: Seq[String] = oneHotEncoderOutputCols ++ continuousAttributes

  private val stringIndexers: Seq[StringIndexer] = {
    val res: ArrayBuffer[StringIndexer] = ArrayBuffer()
    for (d <- discreteStringAttributes) {
      res += new StringIndexer()
        .setInputCol(d)
        .setOutputCol(d + "_indexed")
        .setHandleInvalid("error")
    }
    res
  }

  private val oneHotEncoder: OneHotEncoderEstimator = new OneHotEncoderEstimator()
    .setInputCols(oneHotEncoderInputCols.toArray)
    .setOutputCols(oneHotEncoderOutputCols.toArray)
    .setHandleInvalid("error")


  private val assembler: VectorAssembler = new VectorAssembler()
    .setInputCols(assemblerInputCols.toArray)
    .setOutputCol(featuresCol)


  private val lsh: BucketedRandomProjectionLSH = new BucketedRandomProjectionLSH()
    .setInputCol(featuresCol)
    .setBucketLength(bucketLength)
    .setNumHashTables(numHashTables)

  private def transform(): DataFrame = {
    val stages: Seq[PipelineStage] = stringIndexers ++ Seq(oneHotEncoder, assembler)
    val pipe = new Pipeline().setStages(stages.toArray)
    val model: ml.PipelineModel = pipe.fit(sample)
    model.transform(sample)
  }

  private def child(left: Row, right: Row): Row = {
    var res: Row = left
    for (c <- continuousAttributes) {
      val lc: Double = left.getAs[Double](c)
      val rc: Double = right.getAs[Double](c)
      val diff: Double = rc - lc
      val gap: Double = rand.nextFloat()
      val newValue: Double = lc + (gap * diff)
      res = update(res, c, newValue)
    }
    for (d <- discreteStringAttributes) {
      val ld: String = left.getAs[String](d)
      val rd: String = right.getAs[String](d)
      val newValue: String = rand.nextInt(2) match {
        case 0 => ld
        case _ => rd
      }
      res = update(res, d, newValue)
    }
    for (d <- discreteLongAttributes) {
      val ld: Long = left.getAs[Long](d)
      val rd: Long = right.getAs[Long](d)
      val newValue: Long = rand.nextInt(2) match {
        case 0 => ld
        case _ => rd
      }
      res = update(res, d, newValue)
    }
    res
  }

  def syntheticSample(): DataFrame = {
    val t: DataFrame = transform()
    val model: BucketedRandomProjectionLSHModel = lsh.fit(t)
    val lshDf: DataFrame = model.transform(t)
    val schema = lshDf.schema
    log.info(s"lshDf.count=${lshDf.count}\nlshDf.schema=$schema")
    val rows: Array[Row] = lshDf.collect()
    val res: ArrayBuffer[Row] = ArrayBuffer()
    for (row <- rows) {
      val key: Vector = row.getAs[Vector](featuresCol)
      for (_ <- 1 until sizeMultiplier) {
        val knn: Array[Row] = model.approxNearestNeighbors(
          dataset = lshDf,
          key = key,
          numNearestNeighbors = numNearestNeighbours
        ).toDF().collect()
        val nn: Row = knn(rand.nextInt(knn.length))
        res += child(row, nn)
      }
    }
    toDF(res.toArray, schema).selectExpr(allAttributes: _*)
  }

}
