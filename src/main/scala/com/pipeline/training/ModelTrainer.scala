package com.pipeline.training

import com.pipeline.config.AppConfig
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object ModelTrainer {

  private val log = LoggerFactory.getLogger(getClass)

  // ── Pipeline construction (exposed for testing) ────────────────────────────

  def buildPipeline(): Pipeline = {
    val tokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")
      .setPattern("\\W+")
      .setMinTokenLength(2)

    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("filteredTokens")

    val hashingTF = new HashingTF()
      .setInputCol("filteredTokens")
      .setOutputCol("rawFeatures")
      .setNumFeatures(10000)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setMinDocFreq(2)

    // Encodes labels by frequency: "neutral" → 0.0 / "positive" → 1.0 / "negative" → 2.0
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .setHandleInvalid("skip")

    val lr = new LogisticRegression()
      .setLabelCol("labelIndex")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setRegParam(0.01)

    new Pipeline().setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, labelIndexer, lr))
  }

  // ── Training run ───────────────────────────────────────────────────────────

  def run(spark: SparkSession): Unit = {
    log.info("Loading training data from {}", AppConfig.Data.csvPath)

    // Financial PhraseBank CSV (Kaggle) has columns: Sentence, Sentiment
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv(AppConfig.Data.csvPath)
      .select(
        col("Sentence").as("text"),
        col("Sentiment").as("label")
      )
      .na.drop()

    log.info(s"Dataset: ${df.count()} rows")
    df.groupBy("label").count().orderBy("label").show()

    val Array(trainDf, testDf) = df.randomSplit(Array(0.8, 0.2), seed = 42L)
    log.info(s"Train: ${trainDf.count()} | Test: ${testDf.count()}")

    log.info("Fitting pipeline...")
    val model = buildPipeline().fit(trainDf)

    // ── Evaluate ─────────────────────────────────────────────────────────────
    val predictions = model.transform(testDf)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("labelIndex")
      .setPredictionCol("prediction")

    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val f1       = evaluator.setMetricName("f1").evaluate(predictions)

    log.info(f"Accuracy : ${accuracy * 100}%.2f%%")
    log.info(f"F1 Score : ${f1 * 100}%.2f%%")

    // ── Save ──────────────────────────────────────────────────────────────────
    model.write.overwrite().save(AppConfig.Model.savePath)
    log.info("Model saved to {}", AppConfig.Model.savePath)
  }

  // ── Entry point ───────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master(AppConfig.Spark.master)
      .appName(AppConfig.Spark.appName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    run(spark)
    spark.stop()
  }
}