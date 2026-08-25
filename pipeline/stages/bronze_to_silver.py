"""bronze -> silver : reconciliation des schemas TLC successifs en un modele canonique unique.

Le drift reel du dataset TLC :
- schema legacy (2009) : vendor_name, Trip_Pickup_DateTime, Start_Lon/Start_Lat,
  Fare_Amt, surcharge, Tip_Amt... (horodatages en chaine)
- schema moderne (2016+) : VendorID, tpep_/lpep_pickup_datetime, PULocationID/DOLocationID...
  (depuis 07/2016 la TLC ne publie plus de GPS, uniquement des identifiants de zone)
- fhv : base de dispatch uniquement, ni tarif ni distance.

Les colonnes tarifaires apparues progressivement (improvement_surcharge, congestion_surcharge,
airport_fee, cbd_convenience_fee) sont remplies a NULL quand le fichier source ne les contient
pas : une valeur manquante AVANT l'introduction d'un supplement n'est pas un 0 APRES.

Union par nom avec schéma explicite identique pour chaque fichier (reduce + unionByName),
validation temporelle des horodatages corrompus, ecriture Parquet partitionnee par
vehicle_type/year/month.
"""
import sys
from functools import reduce

from pyspark.sql import DataFrame, functions as F, types as T

from pipeline import config
from pipeline.utils_s3 import client, list_keys, load_manifests
from pipeline.utils_spark import get_spark, lake_uri

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Colonnes canoniques et leur type cible. Toute colonne source absente -> lit(None).
CANONICAL = {
    "vendor": T.StringType(),
    "pickup_ts": T.TimestampType(),
    "dropoff_ts": T.TimestampType(),
    "passenger_count": T.IntegerType(),
    "trip_distance": T.DoubleType(),
    # Localisation : GPS brut avant 07/2016, identifiants de zone ensuite.
    "pickup_lon": T.DoubleType(),
    "pickup_lat": T.DoubleType(),
    "dropoff_lon": T.DoubleType(),
    "dropoff_lat": T.DoubleType(),
    "pulocation_id": T.IntegerType(),
    "dolocation_id": T.IntegerType(),
    # Montants : null = supplement pas encore introduit a cette date.
    "fare_amount": T.DoubleType(),
    "extra": T.DoubleType(),
    "mta_tax": T.DoubleType(),
    "improvement_surcharge": T.DoubleType(),
    "congestion_surcharge": T.DoubleType(),
    "airport_fee": T.DoubleType(),
    "cbd_convenience_fee": T.DoubleType(),
    "tip_amount": T.DoubleType(),
    "tolls_amount": T.DoubleType(),
    "total_amount": T.DoubleType(),
    "payment_type": T.StringType(),
}

# Schema legacy 2009 : premier candidat present dans le fichier qui gagne.
RENAME_LEGACY = {
    "vendor": ["vendor_name"],
    "pickup_ts": ["Trip_Pickup_DateTime"],
    "dropoff_ts": ["Trip_Dropoff_DateTime"],
    "passenger_count": ["Passenger_Count"],
    "trip_distance": ["Trip_Distance"],
    "pickup_lon": ["Start_Lon"],
    "pickup_lat": ["Start_Lat"],
    "dropoff_lon": ["End_Lon"],
    "dropoff_lat": ["End_Lat"],
    "payment_type": ["Payment_Type"],
    "fare_amount": ["Fare_Amt"],
    "extra": ["surcharge"],
    "mta_tax": ["mta_tax"],
    "tip_amount": ["Tip_Amt"],
    "tolls_amount": ["Tolls_Amt"],
    "total_amount": ["Total_Amt"],
}

# Schema moderne (yellow/green/fhv/fhvhv) : couvre les variantes tpep_/lpep_, fhv
# (pickup_datetime / dropOff_datetime / PUlocationID) et fhvhv (trip_miles,
# base_passenger_fare, tips, tolls, hvfhs_license_num). Les montants sans equivalent
# reel (bcf, sales_tax du fhvhv) restent a null plutot que falsifies.
RENAME_MODERN = {
    "vendor": ["VendorID", "dispatching_base_num", "hvfhs_license_num"],
    "pickup_ts": ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"],
    "dropoff_ts": ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropOff_datetime"],
    "passenger_count": ["passenger_count"],
    "trip_distance": ["trip_distance", "trip_miles"],
    "pulocation_id": ["PULocationID", "PUlocationID"],
    "dolocation_id": ["DOLocationID", "DOlocationID"],
    "payment_type": ["payment_type"],
    "fare_amount": ["fare_amount", "base_passenger_fare"],
    "extra": ["extra"],
    "mta_tax": ["mta_tax"],
    "improvement_surcharge": ["improvement_surcharge"],
    "congestion_surcharge": ["congestion_surcharge"],
    "airport_fee": ["airport_fee"],
    "cbd_convenience_fee": ["cbd_convenience_fee"],
    "tip_amount": ["tip_amount", "tips"],
    "tolls_amount": ["tolls_amount", "tolls"],
    "total_amount": ["total_amount"],
}


def normalize(raw: DataFrame, vehicle_type: str, year: int, month: int) -> DataFrame:
    cols = set(raw.columns)
    renames = RENAME_LEGACY if "vendor_name" in cols else RENAME_MODERN
    parse_ts = "vendor_name" in cols

    selected = []
    for canon, dtype in CANONICAL.items():
        sources = [c for c in renames.get(canon, []) if c in cols]
        if not sources:
            col = F.lit(None)
        elif dtype == T.TimestampType() and parse_ts:
            col = F.to_timestamp(sources[0], TS_FORMAT)
        else:
            col = F.col(sources[0])
        selected.append(col.cast(dtype).alias(canon))
    df = raw.select(*selected)
    return (
        df.withColumn("vehicle_type", F.lit(vehicle_type))
          .withColumn("year", F.lit(year))
          .withColumn("month", F.lit(month))
    )


def build_weather(spark, s3):
    """bronze/weather/<annee>/<mois>.json (exports Open-Meteo bruts) -> silver/weather
    en Parquet partitionne par annee. Horodatages deja en heure locale America/New_York,
    alignes sur pickup_ts des courses."""
    keys = sorted(k for k in list_keys(s3, config.BUCKET_BRONZE, "weather/")
                  if k.endswith(".json"))
    if not keys:
        print(">> silver/weather : aucun export meteo en bronze, etape ignoree.")
        return
    frames = []
    for key in keys:
        year = int(key.split("/")[1])
        raw = spark.read.json(lake_uri(config.BUCKET_BRONZE, key))
        frames.append(
            raw.select(F.explode(F.arrays_zip(
                "hourly.time", "hourly.temperature_2m", "hourly.precipitation",
                "hourly.snowfall", "hourly.weather_code", "hourly.wind_speed_10m"
            )).alias("h"))
              .select(
                  F.to_timestamp("h.time", "yyyy-MM-dd'T'HH:mm").alias("ts"),
                  F.col("h.temperature_2m").cast("double").alias("temperature"),
                  F.col("h.precipitation").cast("double").alias("precipitation"),
                  F.col("h.snowfall").cast("double").alias("snowfall"),
                  F.col("h.weather_code").cast("int").alias("weather_code"),
                  F.col("h.wind_speed_10m").cast("double").alias("wind_speed"),
                  F.lit(year).alias("year"))
        )
    weather = reduce(DataFrame.unionByName, frames)
    out = f"{lake_uri(config.BUCKET_SILVER, config.DATASET_ID)}/weather"
    weather.write.mode("overwrite").partitionBy("year").parquet(out)
    total = spark.read.parquet(out).count()
    print(f">> silver/{config.DATASET_ID}/weather ecrit ({total:,} heures, "
          f"{len(keys)} mois sources)")


def main():
    spark = get_spark("nyc-taxi-bronze-to-silver")
    s3 = client()

    if "--weather-only" in sys.argv:
        build_weather(spark, s3)
        return

    manifests = [m for m in load_manifests(s3) if m.get("kind") == "trips"]
    if not manifests:
        print("[!] Aucun manifeste trips dans bronze/. Lance d'abord l'etape ingest.")
        sys.exit(1)

    entries = [f for m in manifests for f in m["files"]]
    print(f">> silver <- bronze [{config.DATASET_ID}] : {len(entries)} fichiers")

    frames = []
    for entry in entries:
        raw = spark.read.parquet(lake_uri(config.BUCKET_BRONZE, entry["key"]))
        frames.append(normalize(raw, entry["vehicle_type"], entry["year"], entry["month"]))
    trips = reduce(DataFrame.unionByName, frames)

    # Validation temporelle : le brut contient des horodatages corrompus.
    # On borne a la fenetre couverte par les fichiers sources.
    lo, hi = min(e["year"] for e in entries), max(e["year"] for e in entries)
    before = trips.count()
    trips = trips.filter(
        (F.col("pickup_ts") >= F.to_timestamp(F.lit(f"{lo}-01-01 00:00:00"), TS_FORMAT))
        & (F.col("pickup_ts") <= F.to_timestamp(F.lit(f"{hi}-12-31 23:59:59"), TS_FORMAT))
    )
    print(f"   - validation dates {lo}-01-01 → {hi}-12-31 : "
          f"{before - trips.count():,} lignes hors fenetre ecartees")

    out = f"{lake_uri(config.BUCKET_SILVER, config.DATASET_ID)}/trips"
    trips.write.mode("overwrite").partitionBy("vehicle_type", "year", "month").parquet(out)

    total = spark.read.parquet(out).count()
    print(f">> silver/{config.DATASET_ID}/trips ecrit en Parquet partitionne par "
          f"vehicle_type/year/month : {total:,} courses")

    build_weather(spark, s3)


if __name__ == "__main__":
    main()
