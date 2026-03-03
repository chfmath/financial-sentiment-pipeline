package com.pipeline.inference

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InferenceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("InferenceSpec")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  // Simulate the raw Kafka DataFrame structure (value column as binary/string)
  private def kafkaLikeDf(jsonRows: String*) = {
    val s = spark
    import s.implicits._
    jsonRows.toSeq.toDF("value")
      .selectExpr("CAST(value AS BINARY) AS value")
  }

  "parseNewsJson" should "extract headline, timestamp and source from valid JSON" in {
    val df = kafkaLikeDf(
      """{"headline":"Apple beats earnings","timestamp":1706000000000,"source":"Yahoo"}"""
    )
    val result = StreamingInference.parseNewsJson(df)
    result.count() shouldEqual 1
    result.columns should contain allOf ("text", "ingestedAt", "source", "label")
    val row = result.first()
    row.getAs[String]("text")      shouldBe "Apple beats earnings"
    row.getAs[Long]("ingestedAt")  shouldEqual 1706000000000L
    row.getAs[String]("source")    shouldBe "Yahoo"
    row.getAs[String]("label")     shouldBe "neutral"  // dummy column always "neutral"
  }

  it should "filter out rows with a null headline" in {
    val df = kafkaLikeDf(
      """{"headline":null,"timestamp":1706000000000,"source":"Reuters"}""",
      """{"headline":"Markets rise on rate cut hopes","timestamp":1706000001000,"source":"Reuters"}"""
    )
    StreamingInference.parseNewsJson(df).count() shouldEqual 1
  }

  it should "filter out rows with an empty headline" in {
    val df = kafkaLikeDf(
      """{"headline":"","timestamp":1706000000000,"source":"Bloomberg"}""",
      """{"headline":"Fed holds rates steady","timestamp":1706000001000,"source":"Bloomberg"}"""
    )
    StreamingInference.parseNewsJson(df).count() shouldEqual 1
  }

  it should "return empty DataFrame for malformed JSON" in {
    val df = kafkaLikeDf("not-valid-json", "also-not-json")
    StreamingInference.parseNewsJson(df).count() shouldEqual 0
  }

  it should "parse multiple valid rows correctly" in {
    val df = kafkaLikeDf(
      """{"headline":"Oil prices surge","timestamp":1706000000000,"source":"Reuters"}""",
      """{"headline":"Tech stocks rally","timestamp":1706000001000,"source":"Bloomberg"}""",
      """{"headline":"Dollar weakens","timestamp":1706000002000,"source":"FT"}"""
    )
    val result = StreamingInference.parseNewsJson(df)
    result.count() shouldEqual 3
    result.select("text").collect().map(_.getString(0)) should contain allOf (
      "Oil prices surge",
      "Tech stocks rally",
      "Dollar weakens"
    )
  }

  it should "produce a schema with expected column types" in {
    val df = kafkaLikeDf(
      """{"headline":"Inflation data released","timestamp":1706000000000,"source":"AP"}"""
    )
    val schema = StreamingInference.parseNewsJson(df).schema
    schema("text").dataType      shouldBe StringType
    schema("ingestedAt").dataType shouldBe LongType
    schema("source").dataType    shouldBe StringType
    schema("label").dataType     shouldBe StringType
  }
}