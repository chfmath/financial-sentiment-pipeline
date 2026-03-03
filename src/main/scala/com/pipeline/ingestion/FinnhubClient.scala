package com.pipeline.ingestion

import com.pipeline.config.AppConfig
import com.pipeline.models.MarketNews
import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Polls the Finnhub REST news endpoint every 60 seconds and publishes new
  * headlines to the `financial-news` Kafka topic as JSON-serialised MarketNews.
  *
  * Only the REST endpoint is used — it is free-tier compatible and delivers
  * real financial news headlines suitable for sentiment analysis.
  *
  * Deduplication is maintained in-memory per session: each article (by id) is
  * emitted to Kafka exactly once, even if it reappears in subsequent polls.
  */
object FinnhubClient {

  private val log = LoggerFactory.getLogger(getClass)

  // ── Finnhub REST news API model ───────────────────────────────────────────

  // Returned by GET /api/v1/news?category=general
  private case class FinnhubRestNewsItem(id: Long, headline: String, datetime: Long, source: String)

  private implicit val restNewsDecoder: Decoder[FinnhubRestNewsItem] = deriveDecoder

  // ── REST news polling source ──────────────────────────────────────────────

  /** Builds a Pekko Streams Source that ticks every 60 seconds, fetches the
    * Finnhub general news feed, and emits only articles not yet seen this session.
    *
    * The `source` field on each MarketNews is the original publisher name
    * (e.g. "Reuters", "Bloomberg", "Yahoo Finance") as returned by Finnhub.
    */
  private def buildNewsPollingSource()(implicit
    system: ActorSystem[Nothing],
    mat:    Materializer,
    ec:     ExecutionContext
  ): Source[MarketNews, _] = {

    val newsUrl =
      s"https://finnhub.io/api/v1/news?category=general&token=${AppConfig.Finnhub.token}"

    Source
      .tick(0.seconds, 60.seconds, ())
      .mapAsync(1) { _ =>
        Http()
          .singleRequest(HttpRequest(uri = newsUrl))
          .flatMap(resp => Unmarshal(resp.entity).to[String])
          .map { body =>
            decode[List[FinnhubRestNewsItem]](body).fold(
              err   => { log.warn("REST news parse error: {}", err.getMessage); Nil },
              items => items
            )
          }
          .recover { case ex =>
            log.warn("REST news poll failed: {}", ex.getMessage)
            Nil
          }
      }
      // Stateful deduplication: each article id is emitted at most once per session
      .statefulMapConcat { () =>
        val seen = scala.collection.mutable.Set.empty[Long]
        items => items.filter(item => seen.add(item.id))
      }
      .map(item => MarketNews(
        headline  = item.headline,
        timestamp = item.datetime * 1000L, // Finnhub returns seconds; MarketNews uses milliseconds
        source    = item.source
      ))
  }

  // ── Entry point ───────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] =
      ActorSystem(Behaviors.empty, "FinnhubIngestion")
    implicit val mat: Materializer    = Materializer(system)
    implicit val ec:  ExecutionContext = system.executionContext

    val producer = NewsKafkaProducer.build()

    val done: Future[Done] = buildNewsPollingSource()
      .map { news =>
        log.info("→ Kafka [{}]: {}", news.source, news.headline.take(80))
        news.asJson.noSpaces
      }
      .runWith(NewsKafkaProducer.sink(producer))

    done.onComplete {
      case Success(_)  => log.info("Ingestion stream completed")
      case Failure(ex) => log.error("Ingestion stream failed", ex)
    }

    sys.addShutdownHook {
      log.info("Shutting down — closing Kafka producer")
      producer.close()
      system.terminate()
    }

    Await.result(system.whenTerminated, Duration.Inf)
  }
}