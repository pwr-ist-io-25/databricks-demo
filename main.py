import os
import urllib.request
import tempfile
from pathlib import Path

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


DATA_URL: str = "https://www.football-data.co.uk/mmz4281/2526/E0.csv"
IS_DATABRICKS: bool = "DATABRICKS_RUNTIME_VERSION" in os.environ

builder = SparkSession.builder.appName("databricks-demo")

if not IS_DATABRICKS:
    builder = builder.master("local[*]")  # Use all available cores when run locally

# Ensure the SparkSession exists
spark: SparkSession = builder.getOrCreate()


def run() -> None:
    # data_dir = _get_current_dir() / "data"
    # data_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "E0.csv"
        print(f"[Local] Fetching data to: {tmp_path.as_posix()}")
        _ = urllib.request.urlretrieve(DATA_URL, tmp_path)

        dbfs_target_path = "dbfs:/databricks-demo/E0.csv"
        dbutils = DBUtils(spark)
        print(f"[Databricks] Copying from local driver to DBFS: {dbfs_target_path}")
        _ = dbutils.fs.cp(f"file:{tmp_path}", dbfs_target_path)

        df = spark.read.csv(
            path=dbfs_target_path, 
            header=True, 
            inferSchema=True,
        )

    df.show(10)


# def _get_current_dir() -> Path:
#     try:
#         return Path(__file__).parent
#     except NameError:
#         # REPL mode
#         return Path(os.getcwd())


if __name__ == "__main__":
    run()
