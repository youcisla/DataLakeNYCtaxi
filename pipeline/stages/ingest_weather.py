"""Ingestion meteo (stub) : pousse des exports locaux (JSON Open-Meteo / NOAA poses dans
data/weather/) vers bronze/weather/<annee>/<fichier>, sans transformation.

L'appel API complet (historique horaire New York) est une etape ulterieure ; l'interface
est volontairement alignee sur ingest_trips : idempotent, --sample N, manifeste cumule
bronze/_manifest/weather.json.
"""
import argparse
import json
import re
import sys
from pathlib import Path

from pipeline import config
from pipeline.utils_s3 import client, put_json

YEAR_RE = re.compile(r"(?P<year>\d{4})")
MANIFEST_KEY = f"{config.MANIFEST_PREFIX}/weather.json"


def discover(raw_dir: Path):
    weather_dir = raw_dir / "weather"
    for path in sorted(weather_dir.glob("*.json")):
        m = YEAR_RE.search(path.stem)
        if not m:
            continue
        yield path, int(m.group("year"))


def main():
    ap = argparse.ArgumentParser(description="Ingestion meteo (stub) vers bronze/weather/.")
    ap.add_argument("--sample", type=int, default=0,
                    help="limiter aux N premiers fichiers")
    args = ap.parse_args()

    found = list(discover(Path(config.RAW_DIR)))
    if not found:
        print(f"[!] Aucun .json meteo dans {Path(config.RAW_DIR) / 'weather'} "
              f"(depot manuel d'un export Open-Meteo/NOAA attendu). Etape ignoree.")
        return
    if args.sample > 0:
        found = found[:args.sample]

    s3 = client()
    try:
        obj = s3.get_object(Bucket=config.BUCKET_BRONZE, Key=MANIFEST_KEY)
        manifest = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        manifest = {"dataset_id": "weather", "kind": "weather", "files": []}
    known_keys = {f["key"] for f in manifest["files"]}

    kept = []
    for path, year in found:
        key = f"weather/{year}/{path.name}"
        if key in known_keys:
            continue
        size = path.stat().st_size
        s3.upload_file(str(path), config.BUCKET_BRONZE, key)
        kept.append({"key": key, "size": size, "year": year})
        print(f"   - bronze/{key} ({size:,} octets)")

    files = {f["key"]: f for f in manifest["files"]}
    files.update({f["key"]: f for f in kept})
    all_files = sorted(files.values(), key=lambda f: f["key"])
    manifest["files"] = all_files
    put_json(s3, config.BUCKET_BRONZE, MANIFEST_KEY, manifest)
    print(f">> Manifeste : bronze/{MANIFEST_KEY} ({len(all_files)} fichiers cumules)")


if __name__ == "__main__":
    main()
