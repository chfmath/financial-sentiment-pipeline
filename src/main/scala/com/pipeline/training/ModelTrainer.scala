package com.pipeline.training

import com.pipeline.config.AppConfig
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.feature.StringIndexerModel
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
      .setWeightCol("weight")
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

    // Compute inverse-frequency class weights so minority classes (positive, negative)
    // contribute equally to the loss despite the dataset being ~60% neutral.
    val classCounts = trainDf.groupBy("label").count().collect()
      .map(r => r.getString(0) -> r.getLong(1)).toMap
    val total      = classCounts.values.sum.toDouble
    val numClasses = classCounts.size.toDouble
    val weightMap  = classCounts.map { case (label, count) =>
      label -> total / (numClasses * count.toDouble)
    }

    log.info("Class weights: {}", weightMap.map { case (k, v) => f"$k→$v%.2f" }.mkString(", "))

    val weightUdf       = udf((label: String) => weightMap.getOrElse(label, 1.0))
    val weightedTrainDf = trainDf.withColumn("weight", weightUdf(col("label")))

    log.info("Fitting pipeline...")
    val model = buildPipeline().fit(weightedTrainDf)

    // ── Evaluate ─────────────────────────────────────────────────────────────
    val predictions = model.transform(testDf)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("labelIndex")
      .setPredictionCol("prediction")

    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val f1       = evaluator.setMetricName("f1").evaluate(predictions)

    log.info(f"Accuracy           : ${accuracy * 100}%.2f%%")
    log.info(f"F1 Score (weighted): ${f1 * 100}%.2f%%")

    // Per-class F1 (label indices from StringIndexer: 0=neutral, 1=positive, 2=negative)
    val labels = model.stages(4).asInstanceOf[StringIndexerModel].labelsArray.head
    for (i <- labels.indices) {
      val classF1 = evaluator.setMetricName("fMeasureByLabel").setMetricLabel(i.toDouble).evaluate(predictions)
      log.info(f"F1 [${labels(i)}%-10s] : ${classF1 * 100}%.2f%%")
    }

    // Macro F1 — unweighted average of per-class F1
    val macroF1 = labels.indices.map { i =>
      evaluator.setMetricName("fMeasureByLabel").setMetricLabel(i.toDouble).evaluate(predictions)
    }.sum / labels.length
    log.info(f"Macro F1           : ${macroF1 * 100}%.2f%%")

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