-- Auto-created by docker-entrypoint-initdb.d on first container start.
-- Matches the schema Spark JDBC would generate from StreamingInference output.
-- Column "ingestedAt" is quoted to preserve camelCase (Spark JDBC convention).

CREATE TABLE IF NOT EXISTS sentiment_results (
    headline    TEXT             NOT NULL,
    sentiment   VARCHAR(20)      NOT NULL,
    confidence  DOUBLE PRECISION NOT NULL,
    "ingestedAt" BIGINT          NOT NULL,
    source      VARCHAR(50)      NOT NULL DEFAULT 'unknown'
);

CREATE INDEX IF NOT EXISTS idx_sentiment_results_ingestedat
    ON sentiment_results ("ingestedAt" DESC);