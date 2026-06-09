-- =====================================================================
-- TP 2B - Schema PostgreSQL pour l'ingestion meteo Open-Meteo
-- Deux schemas :
--   silver     : donnee metier nettoyee et exploitable
--   technical  : traçabilite des executions (suivi d'ingestion)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS technical;

-- ---------------------------------------------------------------------
-- Table metier : une observation meteo par ville et par horodatage.
-- Contrainte d'unicite (city, observed_at) -> permet de rejouer le DAG
-- sans creer de doublons (idempotence).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.weather_observations (
    city            TEXT             NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    observed_at     TIMESTAMP        NOT NULL,
    temperature_c   DOUBLE PRECISION,
    wind_speed_kmh  DOUBLE PRECISION,
    weather_code    INTEGER,
    run_id          TEXT,
    ingested_at     TIMESTAMP        DEFAULT now(),
    CONSTRAINT uq_weather_city_time UNIQUE (city, observed_at)
);

-- ---------------------------------------------------------------------
-- Table de suivi d'ingestion : une ligne par execution du DAG.
-- Permet de savoir ce qui a tourne, quand, combien de lignes, et le statut.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS technical.ingestion_runs (
    run_id               TEXT,
    source               TEXT,
    data_interval_start  TIMESTAMP,
    data_interval_end    TIMESTAMP,
    started_at           TIMESTAMP,
    ended_at             TIMESTAMP,
    status               TEXT,
    records_received     INTEGER,
    records_inserted     INTEGER
);
