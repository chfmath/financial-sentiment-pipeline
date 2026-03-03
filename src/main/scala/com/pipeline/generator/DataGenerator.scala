package com.pipeline.generator

import com.pipeline.config.AppConfig
import com.pipeline.ingestion.NewsKafkaProducer
import com.pipeline.models.MarketNews
import io.circe.syntax._
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Random, Success}

/** Publishes the Financial PhraseBank CSV headlines to the `financial-news`
  * Kafka topic in an infinite loop at a configurable rate.
  *
  * Intended as a high-throughput alternative to FinnhubClient for demonstrating
  * the full pipeline under sustained load. Both can run simultaneously — they
  * publish to the same Kafka topic and are distinguished by `source = "synthetic"`
  * vs the real publisher name from FinnhubClient.
  *
  * Rate is controlled via `generator.messages-per-second` in application.conf.
  * The default of 10 msg/s is comfortable for a laptop; raise it to stress-test.
  *
  * Usage:
  *   sbt 'runMain com.pipeline.generator.DataGenerator'
  */
object DataGenerator {

  private val log = LoggerFactory.getLogger(getClass)

  // ── CSV loading (package-private for testing) ─────────────────────────────

  /** Loads the `Sentence` column from the Financial PhraseBank CSV and returns
    * a shuffled vector of non-empty headline strings.
    *
    * Uses SparkSession for correct handling of quoted fields and UTF-8 encoding —
    * the same approach as ModelTrainer, avoiding a separate CSV parsing library.
    * The SparkSession is stopped immediately after loading so it does not
    * conflict with the Pekko ActorSystem used for streaming.
    */
  private[generator] def loadHeadlines(csvPath: String): Vector[String] = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DataGeneratorLoader")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val headlines = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(csvPath)
        .select("Sentence")
        .collect()
        .flatMap(row => Option(row.getString(0)).filter(_.nonEmpty))
        .toVector

      Random.shuffle(headlines)
    } finally {
      spark.stop()
    }
  }

  // ── Entry point ───────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    val messagesPerSecond = AppConfig.Generator.messagesPerSecond
    val csvPath           = AppConfig.Data.csvPath

    log.info("Loading headlines from {}", csvPath)
    val headlines = loadHeadlines(csvPath)
    log.info("Loaded {} headlines — starting firehose at {}/s", headlines.size, messagesPerSecond)

    implicit val system: ActorSystem[Nothing] =
      ActorSystem(Behaviors.empty, "DataGenerator")
    implicit val mat: Materializer    = Materializer(system)
    implicit val ec:  ExecutionContext = system.executionContext

    val producer = NewsKafkaProducer.build()

    val done = Source
      .cycle(() => headlines.iterator)
      .throttle(messagesPerSecond, 1.second)
      .zipWithIndex
      .map { case (headline, idx) =>
        if (idx % 100 == 0) log.info("Published {} messages", idx)
        MarketNews(
          headline  = headline,
          timestamp = System.currentTimeMillis(),
          source    = "synthetic"
        )
      }
      .map { news =>
        log.debug("→ Kafka [synthetic]: {}", news.headline.take(80))
        news.asJson.noSpaces
      }
      .runWith(NewsKafkaProducer.sink(producer))

    done.onComplete {
      case Success(_)  => log.info("DataGenerator stream completed")
      case Failure(ex) => log.error("DataGenerator stream failed", ex)
    }

    sys.addShutdownHook {
      log.info("Shutting down — closing Kafka producer")
      producer.close()
      system.terminate()
    }

    Await.result(system.whenTerminated, Duration.Inf)
  }
}