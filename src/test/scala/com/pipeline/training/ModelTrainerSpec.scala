package com.pipeline.training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ModelTrainerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("ModelTrainerSpec")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  // Small in-memory dataset — no CSV required
  // `spark` is a var so we alias it to a val first: Scala only allows
  // imports from stable identifiers (vals, not vars).
  private def sampleDf = {
    val s = spark
    import s.implicits._
    Seq(
      ("company reported strong quarterly earnings growth",    "positive"),
      ("revenue fell well below analyst expectations",         "negative"),
      ("trading volumes remained flat on tuesday afternoon",   "neutral"),
      ("profits rose sharply on higher consumer demand",       "positive"),
      ("operating losses widened due to weak sales figures",   "negative"),
      ("the stock price was largely unchanged for the day",    "neutral"),
      ("margins improved significantly thanks to cost cuts",   "positive"),
      ("debt burden increased substantially this quarter",     "negative"),
      ("results were broadly in line with forecasts",          "neutral"),
      ("firm secured a major new contract worth millions",     "positive")
    ).toDF("text", "label")
      .withColumn("weight", lit(1.0))
  }

  "buildPipeline" should "create a pipeline with 6 stages" in {
    ModelTrainer.buildPipeline().getStages.length shouldBe 6
  }

  it should "fit on in-memory data without throwing" in {
    noException should be thrownBy ModelTrainer.buildPipeline().fit(sampleDf)
  }

  it should "produce a model whose output contains prediction and probability columns" in {
    val model       = ModelTrainer.buildPipeline().fit(sampleDf)
    val predictions = model.transform(sampleDf)
    predictions.columns should contain allOf ("text", "label", "prediction", "probability")
  }

  it should "output one prediction row per input row" in {
    val model = ModelTrainer.buildPipeline().fit(sampleDf)
    model.transform(sampleDf).count() shouldEqual sampleDf.count()
  }
}