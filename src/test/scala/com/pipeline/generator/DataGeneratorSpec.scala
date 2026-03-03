package com.pipeline.generator

import com.pipeline.config.AppConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataGeneratorSpec extends AnyFlatSpec with Matchers {

  // loadHeadlines spins up a local SparkSession internally — no shared session needed here

  "loadHeadlines" should "return a non-empty vector for the Kaggle CSV" in {
    val headlines = DataGenerator.loadHeadlines(AppConfig.Data.csvPath)
    headlines should not be empty
  }

  it should "contain more than 4000 headlines from the full dataset" in {
    val headlines = DataGenerator.loadHeadlines(AppConfig.Data.csvPath)
    headlines.length should be > 4000
  }

  it should "contain no null or empty strings" in {
    val headlines = DataGenerator.loadHeadlines(AppConfig.Data.csvPath)
    every(headlines) should not be empty
  }

  it should "return strings that look like financial sentences" in {
    val headlines = DataGenerator.loadHeadlines(AppConfig.Data.csvPath)
    // Every headline should be a printable string with at least a few characters
    every(headlines.map(_.length)) should be > 5
  }
}