from pyspark import SparkFiles
from pyspark.sql import SparkSession

DATA_URL: str = "https://www.football-data.co.uk/mmz4281/2526/E0.csv"


# Ensure SparkSession exists
spark = (
    SparkSession.builder
    .appName("databricks-demo")
    .master("local[*]")  # All available cores when running locally
    .getOrCreate()
)


def run() -> None:
    spark.sparkContext.addFile(DATA_URL)

    df = spark.read.csv(
        path="file://" + SparkFiles.get("E0.csv"), 
        header=True, 
        inferSchema=True
    )

    df.show()


if __name__ == "__main__":
    run()
