"""Ingestion meteo : telecharge l'historique horaire Open-Meteo (New York) pour chaque
mois couvert par le manifeste trips, et le pousse TEL QUEL vers bronze/weather/<annee>/<mois>.json.

API archive Open-Meteo, sans cle : https://archive-api.open-meteo.com/v1/archive
- latitude/longitude : centre de NYC (env WEATHER_LAT / WEATHER_LON pour surcharger)
- hourly : temperature_2m, precipitation, snowfall, weather_code, wind_speed_10m
- timezone=America/New_York : les horodatages sont en heure locale, alignes sur pickup_ts

Idempotent : un mois deja present en bronze n'est pas re-telecharge. Manifeste cumule
bronze/_manifest/weather.json (kind=weather), meme convention que ingest_trips.
"""
import argparse
import json
import sys
import time
import urllib.request
import urllib.parse

from pipeline import config
from pipeline.utils_s3 import client, load_manifests, put_json

MANIFEST_KEY = f"{config.MANIFEST_PREFIX}/weather.json"
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = "temperature_2m,precipitation,snowfall,weather_code,wind_speed_10m"


def target_months(manifests):
    months = set()
    for man in manifests:
        if man.get("kind") != "trips":
            continue
        for f in man["files"]:
            months.add((f["year"], f["month"]))
    return sorted(months)


def fetch_month(year: int, month: int) -> dict:
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    params = urllib.parse.urlencode({
        "latitude": config.WEATHER_LAT,
        "longitude": config.WEATHER_LON,
        "start_date": f"{year:04d}-{month:02d}-01",
        "end_date": f"{year:04d}-{month:02d}-{last_day:02d}",
        "hourly": HOURLY,
        "timezone": "America/New_York",
    })
    url = f"{BASE_URL}?{params}"
    last_err = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Open-Meteo {year}-{month:02d} apres 3 essais : {last_err}")


def main():
    ap = argparse.ArgumentParser(description="Ingestion meteo Open-Meteo vers bronze/weather/.")
    ap.add_argument("--years", type=str, default="",
                    help="limiter a ces annees (ex: 2019,2025) ; defaut : annees du manifeste trips")
    args = ap.parse_args()
    forced_years = {int(y) for y in args.years.split(",") if y.strip()} if args.years else None

    s3 = client()
    manifests = load_manifests(s3)
    months = target_months(manifests)
    if forced_years:
        months = [(y, m) for (y, m) in months if y in forced_years]
    if not months:
        print("[!] Aucun mois trips couvert : lance d'abord l'etape ingest.")
        sys.exit(1)

    existing = set()
    try:
        resp = s3.list_objects_v2(Bucket=config.BUCKET_BRONZE, Prefix="weather/")
        for obj in resp.get("Contents", []):
            existing.add(obj["Key"])
    except Exception:
        pass

    try:
        obj = s3.get_object(Bucket=config.BUCKET_BRONZE, Key=MANIFEST_KEY)
        manifest = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        manifest = {"dataset_id": "weather", "kind": "weather", "files": []}
    known_keys = {f["key"] for f in manifest["files"]}

    kept, skipped, failed = [], 0, []
    for year, month in months:
        key = f"weather/{year}/{month:02d}.json"
        if key in known_keys or key in existing:
            skipped += 1
            continue
        try:
            payload = fetch_month(year, month)
        except RuntimeError as e:
            failed.append(str(e))
            continue
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        s3.put_object(Bucket=config.BUCKET_BRONZE, Key=key, Body=body,
                      ContentType="application/json")
        kept.append({"key": key, "size": len(body), "year": year, "month": month})
        print(f"   - bronze/{key} ({len(body):,} octets)")

    files = {f["key"]: f for f in manifest["files"]}
    files.update({f["key"]: f for f in kept})
    all_files = sorted(files.values(), key=lambda f: f["key"])
    manifest["files"] = all_files
    put_json(s3, config.BUCKET_BRONZE, MANIFEST_KEY, manifest)
    print(f">> Manifeste meteo : {len(all_files)} mois cumules "
          f"({len(kept)} telecharges, {skipped} deja presents, {len(failed) } echecs)")
    for err in failed:
        print(f"   [!] {err}")
    if failed:
        sys.exit(2)


if __name__ == "__main__":
    main()
