import os
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, date_format, to_date

DATA_URL: str = "https://www.football-data.co.uk/mmz4281/2526/E0.csv"
IS_DATABRICKS: bool = "DATABRICKS_RUNTIME_VERSION" in os.environ

builder = SparkSession.builder.appName("databricks-demo")

if not IS_DATABRICKS:
    builder = builder.master("local[*]")  # Use all available cores when run locally

# Ensure the SparkSession exists
spark: SparkSession = builder.getOrCreate()

# Custom type aliases
OddsRecord = tuple[str, str, float, float, float]
OddsByDate = dict[str, list[OddsRecord]]


def normalize_team_name(name: str) -> str:
    if not name:
        return ""
    return name.lower().replace("fc ", "").replace(" fc", "").strip()


def load_odds_from_csv(spark_session: SparkSession, csv_path: str) -> OddsByDate:
    print("\n[3B] ==== ROZPOCZYNAM SKANOWANIE PLIKÓW CSV ====")
    odds_by_date: OddsByDate = {}

    try:
        df = spark_session.read.csv(csv_path, header=True, inferSchema=True)

        # coalesce() próbuje pobrać średnie kursy (Avg), a jeśli nie istnieją, pobiera Bet365 (B365)
        processed_df = df.select(
            date_format(to_date(col("Date"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("norm_date"),
            col("HomeTeam").alias("home"),
            col("AwayTeam").alias("away"),
            coalesce(col("AvgH"), col("B365H")).cast("double").alias("avg_h"),
            coalesce(col("AvgD"), col("B365D")).cast("double").alias("avg_d"),
            coalesce(col("AvgA"), col("B365A")).cast("double").alias("avg_a")
        ).filter(
            col("norm_date").isNotNull() &
            col("home").isNotNull() &
            col("away").isNotNull() &
            (col("avg_h") > 0.0) & (col("avg_d") > 0.0) & (col("avg_a") > 0.0)
        )

        # Przesłanie do Drivera wyłącznie oczyszczonych rekordów
        rows = processed_df.collect()

    except Exception as e:
        print(f"[BŁĄD] Nie udało się przetworzyć danych w Sparku: {e}")
        return odds_by_date

    # Budowa finalnego słownika OddsByDate na Driverze
    for r in rows:
        home_norm = normalize_team_name(r.home)
        away_norm = normalize_team_name(r.away)
        
        odds_by_date.setdefault(r.norm_date, []).append(
            (home_norm, away_norm, r.avg_h, r.avg_d, r.avg_a)
        )

    total_records = sum(len(v) for v in odds_by_date.values())
    print((
        f"[3B] ==== ZAKOŃCZONO. W pamięci jest {total_records} kursów z "
        f"{len(odds_by_date)} unikalnych dat. ====\n"
    ))
    return odds_by_date


def run() -> None:
    data_dir = _get_current_dir() / "data"
    data_dir.mkdir(exist_ok=True, parents=True)
    
    file_path = data_dir / "E0.csv"
    print(f"Fetching data to: {file_path.as_posix()}")
    _ = urllib.request.urlretrieve(DATA_URL, file_path)

    spark_csv_path = f"file:{file_path.as_posix()}"

    odds = load_odds_from_csv(spark, spark_csv_path)

    print("=== PRZYKŁADOWE REKORDY ZE SŁOWNIKA (Z TRZECH DAT) ===")
    sample_dates = list(odds.keys())[:3]
    for date in sample_dates:
        print(f"Data: {date}")
        for entry in odds[date]:
            print(f"\t{entry}")
        print("\n")


def _get_current_dir() -> Path:
    try:
        return Path(__file__).parent
    except NameError:
        return Path(os.getcwd())


if __name__ == "__main__":
    run()