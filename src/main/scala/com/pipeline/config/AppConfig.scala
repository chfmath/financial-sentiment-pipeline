package com.pipeline.config

import com.typesafe.config.ConfigFactory

/** Single access point for all application configuration (application.conf). */
object AppConfig {
  private val config = ConfigFactory.load()

  object Spark {
    val master:  String = config.getString("spark.master")
    val appName: String = config.getString("spark.app-name")
  }

  object Data {
    val csvPath: String = config.getString("data.csv-path")
  }

  object Model {
    val savePath: String = config.getString("model.save-path")
  }

  object Kafka {
    val bootstrapServers: String = config.getString("kafka.bootstrap-servers")
    val topic:            String = config.getString("kafka.topic")
  }

  object Database {
    val url:      String = config.getString("database.url")
    val user:     String = config.getString("database.user")
    val password: String = config.getString("database.password")
    val table:    String = config.getString("database.table")
  }

  object Generator {
    val messagesPerSecond: Int = config.getInt("generator.messages-per-second")
  }

  object Finnhub {
    val token: String = config.getString("finnhub.token")
  }
}