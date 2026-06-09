"""TP 2B - Pipeline complet Open-Meteo -> PostgreSQL.

Chaine : recuperation API -> transformation -> chargement PostgreSQL -> trace.

Decoupage (une responsabilite par tache, noms explicites) :
    fetch_weather_data
        >> transform_weather_records
        >> load_weather_to_postgres
        >> write_ingestion_log

Parametrage (pas de hardcode) :
    - villes        : parametre METIER, lu dans la Variable Airflow "weather_cities"
                      (valeur par defaut fournie si la Variable n'existe pas) ;
    - connexion DB  : parametre TECHNIQUE, via une Connexion Airflow (conn_id),
                      donc aucun credential dans le code.
"""

from __future__ import annotations

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# --- Parametres techniques --------------------------------------------------
POSTGRES_CONN_ID = "weather_postgres"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
SOURCE = "open-meteo"

# --- Parametre metier : villes par defaut (surcharge possible via Variable) -
VILLES_PAR_DEFAUT = [
    {"nom": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"nom": "Berlin", "latitude": 52.5200, "longitude": 13.4050},
    {"nom": "Madrid", "latitude": 40.4168, "longitude": -3.7038},
]


def _charger_villes() -> list[dict]:
    """Lit la liste des villes depuis la Variable Airflow, sinon valeur par defaut."""
    villes = Variable.get("weather_cities", default_var=None, deserialize_json=True)
    return villes or VILLES_PAR_DEFAUT


@dag(
    dag_id="openmeteo_daily_weather_ingestion",
    description="TP 2B - Ingestion meteo Open-Meteo vers PostgreSQL (silver + suivi)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tp", "meteo", "api", "postgres"],
)
def openmeteo_daily_weather_ingestion():

    @task
    def fetch_weather_data() -> list[dict]:
        """Appelle l'API Open-Meteo pour chaque ville et renvoie les reponses brutes."""
        villes = _charger_villes()
        payloads = []
        for ville in villes:
            params = {
                "latitude": ville["latitude"],
                "longitude": ville["longitude"],
                "current_weather": True,
            }
            reponse = requests.get(OPEN_METEO_URL, params=params, timeout=30)
            reponse.raise_for_status()
            payloads.append({"ville": ville["nom"], "reponse_api": reponse.json()})
            logger.info("Meteo recuperee pour %s", ville["nom"])
        logger.info("API interrogee pour %s villes", len(payloads))
        return payloads

    @task
    def transform_weather_records(payloads: list[dict]) -> list[dict]:
        """Aplatit les reponses API en lignes propres, une par ville."""
        records = []
        for element in payloads:
            reponse = element["reponse_api"]
            meteo = reponse["current_weather"]
            record = {
                "city": element["ville"],
                "latitude": reponse["latitude"],
                "longitude": reponse["longitude"],
                "observed_at": meteo["time"],
                "temperature_c": meteo["temperature"],
                "wind_speed_kmh": meteo["windspeed"],
                "weather_code": meteo["weathercode"],
            }
            records.append(record)
            logger.info("Record prepare : %s", record)
        return records

    @task
    def load_weather_to_postgres(records: list[dict]) -> dict:
        """Charge les lignes dans silver.weather_observations (upsert idempotent)."""
        run_id = get_current_context()["run_id"]
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        insert_sql = """
            INSERT INTO silver.weather_observations
                (city, latitude, longitude, observed_at,
                 temperature_c, wind_speed_kmh, weather_code, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, observed_at) DO UPDATE SET
                temperature_c  = EXCLUDED.temperature_c,
                wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                weather_code   = EXCLUDED.weather_code,
                run_id         = EXCLUDED.run_id,
                ingested_at    = now();
        """
        lignes = [
            (
                r["city"], r["latitude"], r["longitude"], r["observed_at"],
                r["temperature_c"], r["wind_speed_kmh"], r["weather_code"], run_id,
            )
            for r in records
        ]

        conn = hook.get_conn()
        cur = conn.cursor()
        cur.executemany(insert_sql, lignes)
        conn.commit()
        inserees = cur.rowcount
        cur.close()
        conn.close()

        logger.info("Chargement PostgreSQL : %s recues / %s inserees", len(lignes), inserees)
        return {"records_received": len(lignes), "records_inserted": inserees}

    @task
    def write_ingestion_log(stats: dict) -> None:
        """Ecrit une ligne de suivi dans technical.ingestion_runs."""
        ctx = get_current_context()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        log_sql = """
            INSERT INTO technical.ingestion_runs
                (run_id, source, data_interval_start, data_interval_end,
                 started_at, ended_at, status, records_received, records_inserted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        hook.run(
            log_sql,
            parameters=(
                ctx["run_id"],
                SOURCE,
                ctx["data_interval_start"],
                ctx["data_interval_end"],
                ctx["dag_run"].start_date,
                datetime.now(),
                "success",
                stats["records_received"],
                stats["records_inserted"],
            ),
        )
        logger.info("Ligne de suivi ecrite pour le run %s", ctx["run_id"])

    # Orchestration : chaque etape depend de la precedente
    payloads = fetch_weather_data()
    records = transform_weather_records(payloads)
    stats = load_weather_to_postgres(records)
    write_ingestion_log(stats)


openmeteo_daily_weather_ingestion()
