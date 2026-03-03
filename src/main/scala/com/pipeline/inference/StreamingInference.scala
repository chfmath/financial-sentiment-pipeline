package com.pipeline.inference

import com.pipeline.config.AppConfig
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

object StreamingInference {

  private val log = LoggerFactory.getLogger(getClass)

  // JSON schema matching MarketNews(headline, timestamp, source)
  private val newsSchema: StructType = StructType(Seq(
    StructField("headline",  StringType, nullable = true),
    StructField("timestamp", LongType,   nullable = true),
    StructField("source",    StringType, nullable = true)
  ))

  // ── JSON parsing (package-private for testing) ─────────────────────────────

  /** Parses the Kafka `value` binary column (JSON) into headline/timestamp/source columns.
    * Adds a dummy `label` column so that StringIndexer (stage 4) finds its input column.
    * The label value is irrelevant at inference time — the model only uses `labelIndex`
    * for training; the prediction is taken from the LogisticRegression output directly.
    */
  private[inference] def parseNewsJson(kafkaDf: DataFrame): DataFrame =
    kafkaDf
      .select(col("value").cast(StringType).as("json"))
      .select(from_json(col("json"), newsSchema).as("d"))
      .select(
        col("d.headline").as("text"),
        col("d.timestamp").as("ingestedAt"),
        col("d.source").as("source")
      )
      .filter(col("text").isNotNull && length(col("text")) > 0)
      .withColumn("label", lit("neutral"))   // dummy for StringIndexer input col

  // ── Batch writer to PostgreSQL ─────────────────────────────────────────────

  private def jdbcProps(): Properties = {
    val p = new Properties()
    p.setProperty("user",   AppConfig.Database.user)
    p.setProperty("password", AppConfig.Database.password)
    p.setProperty("driver", "org.postgresql.Driver")
    p
  }

  /** Writes one micro-batch of predictions to the `sentiment_results` PostgreSQL table.
    * Uses `mode("append")` — the table is auto-created on first write.
    */
  private def writeBatch(batchDf: DataFrame, labels: Array[String]): Unit = {
    // Recover human-readable label from numeric prediction index
    val labelMap = udf((idx: Double) => labels.applyOrElse(idx.toInt, (_: Int) => "unknown"))

    val toWrite = batchDf
      .withColumn("sentiment",  labelMap(col("prediction")))
      .withColumn("confidence", array_max(vector_to_array(col("probability"))))
      .select(
        col("text").as("headline"),
        col("sentiment"),
        col("confidence"),
        col("ingestedAt"),
        col("source")
      )

    val rowCount = toWrite.count()

    toWrite.write
      .mode("append")
      .jdbc(AppConfig.Database.url, AppConfig.Database.table, jdbcProps())

    log.info("Wrote {} rows to PostgreSQL", rowCount)
  }

  // ── Entry point ───────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master(AppConfig.Spark.master)
      .appName(AppConfig.Spark.appName + "-Inference")
      // Avoid serialization issues with UDFs
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    log.info("Loading model from {}", AppConfig.Model.savePath)
    val model = PipelineModel.load(AppConfig.Model.savePath)

    // Stage 4 is the StringIndexer — its labels array maps 0→"positive", 1→"negative" etc.
    val labels: Array[String] =
      model.stages(4).asInstanceOf[StringIndexerModel].labelsArray.head

    log.info("Label mapping: {}", labels.zipWithIndex.map { case (l, i) => s"$i→$l" }.mkString(", "))

    // ── Read from Kafka ───────────────────────────────────────────────────────
    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.Kafka.bootstrapServers)
      .option("subscribe",               AppConfig.Kafka.topic)
      .option("startingOffsets",         "latest")
      .option("failOnDataLoss",          "false")
      .load()

    // ── Parse → add dummy label → transform ───────────────────────────────────
    val newsDf = parseNewsJson(kafkaDf)

    val predictions = model.transform(newsDf)

    // ── Write to PostgreSQL via foreachBatch ──────────────────────────────────
    val query = predictions.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        log.info("Processing batch {}", batchId)
        if (!batch.isEmpty) writeBatch(batch, labels)
      }
      .option("checkpointLocation", "./checkpoints/streaming-inference")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .start()

    log.info("Streaming inference started — awaiting Kafka messages")
    query.awaitTermination()
  }
}