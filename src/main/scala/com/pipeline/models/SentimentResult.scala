package com.pipeline.models

/** Row written to PostgreSQL after the Spark Structured Streaming inference job. */
case class SentimentResult(
  headline:   String,
  sentiment:  String,   // "positive" | "negative" | "neutral"
  confidence: Double,
  ingestedAt: Long,     // Unix epoch milliseconds
  source:     String    // "synthetic" or real publisher e.g. "Reuters"
)