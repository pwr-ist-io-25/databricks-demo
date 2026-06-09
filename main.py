import os
import urllib.request
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession


DATA_URL: str = "https://www.football-data.co.uk/mmz4281/2526/E0.csv"
IS_DATABRICKS: bool = "DATABRICKS_RUNTIME_VERSION" in os.environ

builder = SparkSession.builder.appName("databricks-demo")

if not IS_DATABRICKS:
    builder = builder.master("local[*]")  # Use all available cores when run locally

# Ensure the SparkSession exists
spark: SparkSession = builder.getOrCreate()


def run() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "E0.csv"
        print(f"[Databricks] Fetching data to: {local_path.as_posix()}")
        _ = urllib.request.urlretrieve(DATA_URL, local_path)

        df = spark.read.csv(
            path=f"file:{local_path}", 
            header=True, 
            inferSchema=True,
        )

    df.show(10)


if __name__ == "__main__":
    run()
