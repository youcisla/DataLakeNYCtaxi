from pyspark.sql import SparkSession

from pipeline import config


def get_spark(app_name: str) -> SparkSession:
    """Session Spark connectee au master configure (spark://spark-master:7077 en docker).

    En local, SPARK_MASTER_URL=local[*] doit etre pose explicitement (tests LAKE=local).
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master(config.SPARK_MASTER_URL)
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.network.timeout", "300s")
        .getOrCreate()
    )


def lake_uri(bucket: str, key: str) -> str:
    return config.lake_path(bucket, key)
