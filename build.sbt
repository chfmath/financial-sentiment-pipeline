ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

val sparkVersion     = "3.5.3"
val pekkoVersion     = "1.0.3"
val pekkoHttpVersion = "1.0.1"
val circeVersion     = "0.14.9"

lazy val root = (project in file("."))
  .settings(
    name := "financial-sentiment-pipeline",

    libraryDependencies ++= Seq(
      // ── Spark ──────────────────────────────────────────────────────────────
      "org.apache.spark" %% "spark-core"           % sparkVersion,
      "org.apache.spark" %% "spark-sql"            % sparkVersion,
      "org.apache.spark" %% "spark-mllib"          % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

      // ── Pekko ──────────────────────────────────────────────────────────────
      "org.apache.pekko" %% "pekko-actor-typed"      % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream"           % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"             % pekkoHttpVersion,

      // ── JSON ───────────────────────────────────────────────────────────────
      "io.circe" %% "circe-core"    % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser"  % circeVersion,

      // ── Config & Logging ───────────────────────────────────────────────────
      "com.typesafe"   %  "config"          % "1.4.3",
      "ch.qos.logback" %  "logback-classic" % "1.3.14",

      // ── Database ───────────────────────────────────────────────────────────
      "org.postgresql" % "postgresql" % "42.7.3",

      // ── Testing ────────────────────────────────────────────────────────────
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),

    // Spark bundles its own SLF4J binding (log4j2); exclude it so Logback wins cleanly
    excludeDependencies += ExclusionRule("org.apache.logging.log4j", "log4j-slf4j2-impl"),

    // Fork a separate JVM for sbt run/test — required for Spark on Java 17+
    fork      := true,
    Test / fork := true,

    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
    )
  )