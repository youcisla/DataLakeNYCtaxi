"""silver -> gold : agregats metier, un mart JSON par analyse du notebook.

1. Evolution des supplements tarifaires apparus progressivement, par type de vehicule.
2. Flux de prises en charge / deposes entre zones (diagramme en corde).
3. Prix au km par annee et zone de depart (ridgeline plot).
4. Frequence des courses jour de semaine x heure, par annee et type de vehicule.
5. Ecart de prix selon la meteo (en attente de l'ingestion meteo -> payload placeholder).
6. Activite par zone (carte choroplethe) et chronologie mensuelle (filtres croises cote client).
"""
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from pipeline import config
from pipeline.utils_s3 import client, list_keys, load_manifests, put_json
from pipeline.utils_spark import get_spark, lake_uri

JOURS = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]

# Supplements et composants tarifaires apparus progressivement dans le schema TLC.
SUPPLEMENTS = ["extra", "mta_tax", "improvement_surcharge",
               "congestion_surcharge", "airport_fee", "cbd_convenience_fee"]
TOP_ZONES = 20
TOP_LINKS = 100

# Histogrammes prix/km : cases de 1 $/km bornees a 10 $/km (ridgeline).
BIN_WIDTH = 1.0
NBINS = 10


def surcharge_evolution(trips):
    aggs = []
    for s in SUPPLEMENTS:
        aggs += [F.round(F.avg(s), 2).alias(f"{s}_moyen"),
                 F.round(F.sum(s), 2).alias(f"{s}_total")]
    rows = (
        trips.groupBy("year", "vehicle_type").agg(*aggs)
             .orderBy("year", "vehicle_type").collect()
    )
    out = []
    for r in rows:
        row = {"annee": int(r["year"]), "vehicle_type": r["vehicle_type"]}
        row.update({f"{c}_{k}": (float(r[f"{c}_{k}"]) if r[f"{c}_{k}"] is not None else None)
                    for c in SUPPLEMENTS for k in ("moyen", "total")})
        out.append(row)
    return {"supplements": SUPPLEMENTS, "rows": out}


def zone_flows(trips):
    zoned = trips.filter(
        F.col("pulocation_id").isNotNull() & F.col("dolocation_id").isNotNull())
    n_zoned = zoned.count()
    links = [
        {"source": int(r["pu"]), "target": int(r["do"]), "count": int(r["count"])}
        for r in (zoned.groupBy(F.col("pulocation_id").alias("pu"),
                                F.col("dolocation_id").alias("do"))
                  .count().orderBy(F.desc("count")).limit(TOP_LINKS).collect())
    ]
    nodes = [
        {"id": int(r["zone"]), "pickups": int(r["pickups"])}
        for r in (zoned.groupBy(F.col("pulocation_id").alias("zone"))
                  .count().withColumnRenamed("count", "pickups")
                  .orderBy(F.desc("pickups")).limit(TOP_ZONES).collect())
    ]
    return {"nodes": nodes, "links": links, "courses_zonees": n_zoned,
            "courses_non_zonees": trips.count() - n_zoned}


def price_per_km_by_zone(trips):
    ppk = trips.filter(
        (F.col("trip_distance") > 0) & F.col("pulocation_id").isNotNull()
        & (F.col("total_amount").isNotNull())
    ).withColumn("prix_km", F.col("total_amount") / F.col("trip_distance")) \
     .withColumn("bin", F.greatest(F.least(F.floor(F.col("prix_km") / BIN_WIDTH),
                                           F.lit(NBINS - 1)), F.lit(0)).cast("int"))
    top_ids = [int(r["zone"]) for r in (
        ppk.groupBy(F.col("pulocation_id").alias("zone")).count()
           .orderBy(F.desc("count")).limit(TOP_ZONES).collect())]
    if not top_ids:
        return {"bins": [f"{int(i * BIN_WIDTH)}-{int((i + 1) * BIN_WIDTH)}$" for i in range(NBINS)],
                "zones": [], "rows": []}
    rows = []
    for r in (ppk.filter(F.col("pulocation_id").isin(top_ids))
                .groupBy("year", "pulocation_id", "bin")
                .agg(F.count("*").alias("courses"),
                     F.round(F.avg("prix_km"), 2).alias("prix_km_moyen"))
                .collect()):
        rows.append({"annee": int(r["year"]), "zone": int(r["pulocation_id"]),
                     "bin": int(r["bin"]), "courses": int(r["courses"]),
                     "prix_km_moyen": float(r["prix_km_moyen"])})
    # une ligne (annee, zone) -> histogramme complet + moyenne ponderee par les courses
    grouped: dict = {}
    for r in rows:
        g = grouped.setdefault((r["annee"], r["zone"]),
                               {"hist": [0] * NBINS, "somme": 0.0, "n": 0})
        g["hist"][r["bin"]] = r["courses"]
        g["somme"] += r["prix_km_moyen"] * r["courses"]
        g["n"] += r["courses"]
    out = [{"annee": a, "zone": z,
            "prix_km_moyen": round(g["somme"] / g["n"], 2),
            "courses": g["n"], "hist": g["hist"]}
           for (a, z), g in sorted(grouped.items())]
    return {"bins": [f"{int(i * BIN_WIDTH)}-{int((i + 1) * BIN_WIDTH)}$" for i in range(NBINS)],
            "zones": sorted(top_ids), "rows": out}


def activity_heatmap(trips):
    dens = (
        trips.groupBy("year", "vehicle_type",
                      ((F.dayofweek("pickup_ts") + 5) % 7).alias("jour"),
                      F.hour("pickup_ts").alias("heure"))
             .count().collect()
    )
    nested: dict = {}
    years = sorted({int(r["year"]) for r in dens})
    for y in years:
        nested[str(y)] = {}
    for r in dens:
        veh = nested.setdefault(str(r["year"]), {}) \
                    .setdefault(r["vehicle_type"],
                                {"jours": JOURS, "heures": list(range(24)),
                                 "matrice": [[0] * 24 for _ in range(7)]})
        veh["matrice"][int(r["jour"])][int(r["heure"])] = int(r["count"])
    return nested


def zone_stats(trips):
    # Par (zone, annee, vehicule) : departs, arrivees, tarif moyen, prix/km moyen.
    # Les filtres annee x vehicule sont appliques cote client sur ces lignes.
    pu = (
        trips.filter(F.col("pulocation_id").isNotNull())
             .groupBy("pulocation_id", "year", "vehicle_type")
             .agg(F.count("*").alias("pickups"),
                  F.round(F.avg("fare_amount"), 2).alias("tarif_moyen"),
                  F.round(F.avg(F.when((F.col("trip_distance") > 0)
                                       & F.col("total_amount").isNotNull(),
                                       F.col("total_amount") / F.col("trip_distance"))), 2)
                   .alias("prix_km_moyen"))
    )
    do = (
        trips.filter(F.col("dolocation_id").isNotNull())
             .groupBy(F.col("dolocation_id").alias("pulocation_id"), "year", "vehicle_type")
             .agg(F.count("*").alias("dropoffs"))
    )
    joined = pu.join(do, ["pulocation_id", "year", "vehicle_type"], "left")
    rows = [
        {"zone": int(r["pulocation_id"]), "annee": int(r["year"]),
         "vehicle": r["vehicle_type"], "pickups": int(r["pickups"]),
         "dropoffs": int(r["dropoffs"] or 0),
         "tarif_moyen": float(r["tarif_moyen"]) if r["tarif_moyen"] is not None else None,
         "prix_km_moyen": float(r["prix_km_moyen"]) if r["prix_km_moyen"] is not None else None}
        for r in joined.collect()
    ]
    return {"rows": rows}


def monthly_timeline(trips):
    rows = (
        trips.groupBy("year", "month", "vehicle_type").count()
             .orderBy("year", "month", "vehicle_type").collect()
    )
    return {"rows": [{"annee": int(r["year"]), "mois": int(r["month"]),
                      "vehicle": r["vehicle_type"], "courses": int(r["count"])}
                     for r in rows]}


def zone_lookup():
    csv_path = Path(config.RAW_DIR) / "taxi_zone_lookup.csv"
    if not csv_path.exists():
        return {}
    out = {}
    with open(csv_path, encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            out[int(row["LocationID"])] = {"zone": row["Zone"], "borough": row["Borough"]}
    return out


def weather_deviation(spark, s3, trips_base):
    # Jointure course -> meteo a l'heure : les courses sont agregees par heure
    # (somme montants / somme distances), puis rattachees a la ligne meteo horaire.
    weather_keys = list_keys(s3, config.BUCKET_SILVER,
                             f"{config.DATASET_ID}/weather/")
    if not weather_keys:
        return {"statut": "en_attente",
                "message": "Ingestion meteo non effectuee : lance make weather "
                           "(telechargement Open-Meteo) puis make bronze-to-silver."}
    weather = spark.read.parquet(
        f"{lake_uri(config.BUCKET_SILVER, config.DATASET_ID)}/weather")
    trips = spark.read.parquet(trips_base)

    hourly = (
        trips.filter((F.col("trip_distance") > 0) & F.col("total_amount").isNotNull())
             .groupBy("year", "month",
                      F.dayofmonth("pickup_ts").alias("jour"),
                      F.hour("pickup_ts").alias("heure"), "vehicle_type")
             .agg(F.count("*").alias("courses"),
                  F.sum("total_amount").alias("somme_total"),
                  F.sum("trip_distance").alias("somme_km"))
    )
    joined = hourly.join(
        weather.select("ts", "temperature", "weather_code",
                       F.year("ts").alias("year"), F.month("ts").alias("month"),
                       F.dayofmonth("ts").alias("jour"), F.hour("ts").alias("heure")),
        ["year", "month", "jour", "heure"], "inner"
    ).withColumn("prix_km", F.col("somme_total") / F.col("somme_km"))

    def categorie(code):
        return (F.when(code.isNull(), "Inconnu")
                 .when(code == 0, "Dégagé")
                 .when(code < 4, "Nuageux")
                 .when(code < 49, "Brouillard")
                 .when(code < 70, "Pluie")
                 .when(code < 80, "Neige")
                 .when(code < 83, "Averses")
                 .when(code < 95, "Neige")
                 .otherwise("Orage"))

    def bande(temp):
        return (F.when(temp < 0, "< 0°C")
                 .when(temp < 10, "0-10°C")
                 .when(temp < 20, "10-20°C")
                 .when(temp < 30, "20-30°C")
                 .otherwise("≥ 30°C"))

    def agg_rows(df, dim):
        rows = (df.groupBy("year", dim)
                  .agg(F.sum("courses").alias("courses"),
                       F.sum("somme_total").alias("tot"),
                       F.sum("somme_km").alias("km"))
                  .collect())
        return [{"annee": int(r["year"]), dim: r[dim], "courses": int(r["courses"]),
                 "prix_km_moyen": round(float(r["tot"]) / float(r["km"]), 2)}
                for r in rows]

    glob = (joined.groupBy("year")
                  .agg(F.sum("courses").alias("courses"),
                       F.sum("somme_total").alias("tot"),
                       F.sum("somme_km").alias("km")).collect())
    glob_rows = [{"annee": int(r["year"]), "courses": int(r["courses"]),
                  "prix_km_moyen": round(float(r["tot"]) / float(r["km"]), 2)}
                 for r in glob]

    return {
        "statut": "ok",
        "conditions": ["Dégagé", "Nuageux", "Brouillard", "Pluie", "Neige", "Averses", "Orage"],
        "rows": agg_rows(joined.withColumn("condition", categorie(F.col("weather_code"))),
                         "condition"),
        "temp_bandes": ["< 0°C", "0-10°C", "10-20°C", "20-30°C", "≥ 30°C"],
        "temp_rows": agg_rows(joined.withColumn("bande", bande(F.col("temperature"))),
                              "bande"),
        "global_rows": glob_rows,
        "message": "Jointure horaire course × météo Open-Meteo (heure locale NYC).",
    }


def main():
    spark = get_spark("nyc-taxi-silver-to-gold")
    s3 = client()
    manifests = [m for m in load_manifests(s3) if m.get("kind") == "trips"]
    if not manifests:
        print("[!] Aucun manifeste trips dans bronze/. Lance d'abord l'etape ingest.")
        sys.exit(1)

    for man in manifests:
        ds = man["dataset_id"]
        base = f"{lake_uri(config.BUCKET_SILVER, ds)}/trips"
        print(f">> gold <- silver [{ds}]")
        trips = spark.read.parquet(base)

        # ---- KPI globaux -------------------------------------------------
        total = trips.count()
        bornes = trips.agg(
            F.min("pickup_ts").alias("debut"), F.max("pickup_ts").alias("fin")
        ).first()
        par_annee = {str(r["year"]): int(r["count"])
                     for r in trips.groupBy("year").count().orderBy("year").collect()}
        par_vehicule = {r["vehicle_type"]: int(r["count"])
                        for r in trips.groupBy("vehicle_type").count().collect()}
        kpis = {
            "total_courses": total,
            "periode": {
                "debut": bornes["debut"].strftime("%d/%m/%Y") if bornes["debut"] else None,
                "fin": bornes["fin"].strftime("%d/%m/%Y") if bornes["fin"] else None,
            },
            "par_annee": par_annee,
            "par_vehicule": par_vehicule,
        }

        # ---- Analyse 1 : evolution des supplements ------------------------
        supplements = surcharge_evolution(trips)

        # ---- Analyse 2 : flux pickup -> dropoff entre zones ---------------
        flux_zones = zone_flows(trips)

        # ---- Analyse 3 : prix au km par annee x zone de depart ------------
        prix_km = price_per_km_by_zone(trips)

        # ---- Analyse 4 : heatmap jour x heure par annee et vehicle --------
        heatmap = activity_heatmap(trips)

        # ---- Analyse 5 : ecart de prix selon la meteo ---------------------
        meteo = weather_deviation(spark, s3, base)

        # ---- Analyse 6 : activite par zone + chronologie mensuelle --------
        zones_stats = zone_stats(trips)
        timeline = monthly_timeline(trips)

        # ---- Ecriture dans gold ------------------------------------------
        gk = f"{ds}/"
        put_json(s3, config.BUCKET_GOLD, gk + "kpis.json", kpis)
        put_json(s3, config.BUCKET_GOLD, gk + "surcharge_evolution.json", supplements)
        put_json(s3, config.BUCKET_GOLD, gk + "zone_flows.json", flux_zones)
        put_json(s3, config.BUCKET_GOLD, gk + "price_per_km_by_zone.json", prix_km)
        put_json(s3, config.BUCKET_GOLD, gk + "activity_heatmap.json", heatmap)
        put_json(s3, config.BUCKET_GOLD, gk + "weather_deviation.json", meteo)
        put_json(s3, config.BUCKET_GOLD, gk + "zone_stats.json", zones_stats)
        put_json(s3, config.BUCKET_GOLD, gk + "timeline.json", timeline)
        put_json(s3, config.BUCKET_GOLD, gk + "zone_lookup.json", zone_lookup())
        put_json(s3, config.BUCKET_GOLD, gk + "meta.json", {
            "dataset_id": ds,
            "source_dir": man.get("source_dir"),
            "sample_mode": man.get("sample_mode", False),
            "taille_octets": man.get("taille_octets"),
            "spark_master": config.SPARK_MASTER_URL,
            "lake": config.LAKE,
            "buckets": {"bronze": config.BUCKET_BRONZE, "silver": config.BUCKET_SILVER,
                        "gold": config.BUCKET_GOLD},
            "genere_le": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        print(f">> gold/{gk} : kpis + 5 marts analyses ecrits ({total:,} courses, "
              f"vehicules={list(par_vehicule)})")


if __name__ == "__main__":
    main()
