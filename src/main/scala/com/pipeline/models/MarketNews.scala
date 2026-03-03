package com.pipeline.models

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}

/** Canonical domain model for a piece of market-related text that will be
  * pushed to Kafka and later classified by the Spark ML model.
  *
  * @param headline  The text to analyse (news headline or formatted trade tick).
  * @param timestamp Unix epoch milliseconds.
  * @param source    Origin of the data (e.g. "Finnhub", "Yahoo").
  */
case class MarketNews(headline: String, timestamp: Long, source: String)

object MarketNews {
  implicit val encoder: Encoder[MarketNews] = deriveEncoder
  implicit val decoder: Decoder[MarketNews] = deriveDecoder
}