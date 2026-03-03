package com.pipeline.ingestion

import com.pipeline.config.AppConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.Sink
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.concurrent.Future

/** Builds a plain Kafka producer and exposes it as a Pekko Streams Sink. */
object NewsKafkaProducer {

  private val log = LoggerFactory.getLogger(getClass)

  def build(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      AppConfig.Kafka.bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    new KafkaProducer[String, String](props)
  }

  /** A Sink that writes each JSON string to the configured Kafka topic. */
  def sink(producer: KafkaProducer[String, String]): Sink[String, Future[Done]] =
    Sink.foreach[String] { json =>
      producer.send(new ProducerRecord[String, String](AppConfig.Kafka.topic, json))
      log.debug("Sent to Kafka: {}", json.take(120))
    }
}