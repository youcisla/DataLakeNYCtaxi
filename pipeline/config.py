import os
from pathlib import Path


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# LAKE=minio (defaut, mode docker) : stockage objet MinIO via s3a://.
# LAKE=local : systeme de fichiers local sous LAKE_LOCAL_DIR (tests sans Docker,
# avec SPARK_MASTER_URL=local[*] explicite).
LAKE = env("LAKE", "minio")
LOCAL_ROOT = Path(env("LAKE_LOCAL_DIR", "lake_local")).resolve()

SPARK_MASTER_URL = env("SPARK_MASTER_URL", "spark://spark-master:7077")
MINIO_ENDPOINT = env("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ROOT_USER = env("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = env("MINIO_ROOT_PASSWORD", "minioadmin123")

SAMPLE_COUNT = int(env("SAMPLE", "0"))

# Defauts host-friendly ; le compose force /data/raw et /out/site dans le conteneur.
RAW_DIR = env("RAW_DIR", "data")
SITE_DIR = env("SITE_DIR", "dashboard/site")

BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

MANIFEST_PREFIX = "_manifest"

DATASET_ID = "nyc-taxi-trips"

# Centre de NYC pour l'API meteo Open-Meteo (heure locale America/New_York cote API).
WEATHER_LAT = env("WEATHER_LAT", "40.7128")
WEATHER_LON = env("WEATHER_LON", "-74.0060")


def lake_path(bucket: str, key: str) -> str:
    """Chemin d'un objet du lake, en storage objet (s3a://) ou local selon LAKE."""
    if LAKE == "local":
        return str(LOCAL_ROOT / bucket / key)
    return f"s3a://{bucket}/{key}"


def slugify(name: str) -> str:
    keep = "".join(c.lower() if c.isalnum() else "-" for c in name)
    return "-".join(p for p in keep.split("-") if p)
