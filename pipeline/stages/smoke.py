"""Test de bout en bout du cluster : ecriture/lecture Parquet sur MinIO via le vrai master."""
from pyspark.sql import functions as F

from pipeline import config
from pipeline.utils_spark import get_spark, lake_uri

PATH = f"{lake_uri(config.BUCKET_SILVER, config.DATASET_ID)}/_smoke/roundtrip"


def main():
    spark = get_spark("nyc-taxi-smoke")
    print(f">> master : {config.SPARK_MASTER_URL}")
    print(f">> lake : {config.LAKE} ({config.MINIO_ENDPOINT if config.LAKE == 'minio' else config.LOCAL_ROOT})")

    spark.range(1000).withColumn("x", F.col("id") * 2) \
        .write.mode("overwrite").parquet(PATH)
    count = spark.read.parquet(PATH).count()
    assert count == 1000, f"roundtrip invalide : {count}"

    slots = spark.sparkContext.defaultParallelism
    print(f">> cœurs d'exécution vus par le driver : {slots}")
    print(f">> SMOKE OK : 1000 lignes ecrites puis relues via {PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
