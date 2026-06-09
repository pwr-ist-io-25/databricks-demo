import os
import urllib.request
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
    data_dir = _get_current_dir() / "data"
    data_dir.mkdir(exist_ok=True, parents=True)
    
    file_path = data_dir / "E0.csv"
    print(f"Fetching data to: {file_path.as_posix()}")
    _ = urllib.request.urlretrieve(DATA_URL, file_path)

    df = spark.read.csv(
        path=f"file:{file_path.as_posix()}",
        header=True,
        inferSchema=True,
    )
    df.show(10)


def _get_current_dir() -> Path:
    try:
        return Path(__file__).parent
    except NameError:
        # REPL mode
        return Path(os.getcwd())


if __name__ == "__main__":
    run()
