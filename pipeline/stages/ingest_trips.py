"""Ingestion : copie le brut TEL QUEL vers le bucket bronze (MinIO) avant toute transformation.

Convention de chemins bronze/<vehicle_type>/<year>/<month>/<vehicle>_tripdata_<year>-<month>.parquet :
elle repond sans ouvrir les fichiers a
- quels types de vehicules sont deja ingeres ;
- pour quelles periodes (annee/mois) un type est couvert ;
- si un mois precis d'un type precis a deja ete ingere.

Le script est idempotent : ce qui existe deja en bucket n'est pas re-pousse.
Un manifeste JSON decrit l'etat cumule -> bronze/_manifest/<dataset>.json

--sample N limite l'ingestion aux N premiers fichiers (iterations rapides).
"""
import argparse
import json
import re
import sys
from pathlib import Path

from pipeline import config
from pipeline.utils_s3 import client, list_keys, put_json

FILENAME_RE = re.compile(r"(?P<vehicle>[a-z]+)_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.parquet$")


def discover(raw_dir: Path):
    for path in sorted(raw_dir.rglob("*_tripdata_*.parquet")):
        m = FILENAME_RE.search(path.name)
        if not m:
            continue
        yield path, m.group("vehicle"), int(m.group("year")), int(m.group("month"))


def load_manifest(s3):
    key = f"{config.MANIFEST_PREFIX}/{config.DATASET_ID}.json"
    try:
        obj = s3.get_object(Bucket=config.BUCKET_BRONZE, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return {"dataset_id": config.DATASET_ID, "kind": "trips", "source_dir": None,
                "sample_mode": False, "files": [], "taille_octets": 0}


def main():
    ap = argparse.ArgumentParser(description="Ingestion des courses TLC vers bronze.")
    ap.add_argument("--sample", type=int, default=config.SAMPLE_COUNT,
                    help="limiter aux N premiers fichiers")
    args = ap.parse_args()

    raw_dir = Path(config.RAW_DIR)
    found = list(discover(raw_dir))
    if not found:
        print(f"[!] Aucun *_tripdata_*.parquet dans {raw_dir}. Verifie DATA_PATH.")
        sys.exit(1)
    if args.sample > 0:
        found = found[:args.sample]

    mode = f"SAMPLE ({len(found)} fichiers)" if args.sample > 0 else "COMPLET"
    print(f">> Ingestion [{mode}] de {len(found)} fichiers depuis {raw_dir} "
          f"-> s3a://{config.BUCKET_BRONZE}/{config.DATASET_ID}/")

    s3 = client()
    existing = set(list_keys(s3, config.BUCKET_BRONZE, f"{config.DATASET_ID}/"))
    manifest = load_manifest(s3)
    known_keys = {f["key"] for f in manifest["files"]}

    kept, skipped, healed = [], 0, 0
    for path, vehicle, year, month in found:
        key = f"{config.DATASET_ID}/{vehicle}/{year}/{month:02d}/{path.name}"
        size = path.stat().st_size
        if key in known_keys:
            skipped += 1
            continue
        if key in existing:
            # Objet deja en bucket mais absent du manifeste (run interrompu avant
            # l'ecriture du manifeste) : on le re-registry sans le re-pousser.
            healed += 1
            kept.append({"key": key, "size": size,
                         "vehicle_type": vehicle, "year": year, "month": month})
            print(f"   - bronze/{key} deja en bucket, rajoute au manifeste")
            continue
        s3.upload_file(str(path), config.BUCKET_BRONZE, key)
        kept.append({"key": key, "size": size,
                     "vehicle_type": vehicle, "year": year, "month": month})
        print(f"   - bronze/{key} ({size:,} octets)")

    files = {f["key"]: f for f in manifest["files"]}
    files.update({f["key"]: f for f in kept})
    all_files = sorted(files.values(), key=lambda f: f["key"])
    manifest.update({
        "source_dir": str(raw_dir),
        "sample_mode": args.sample > 0,
        "files": all_files,
        "trip_years": sorted({f["year"] for f in all_files}),
        "taille_octets": sum(f["size"] for f in all_files),
    })
    put_json(s3, config.BUCKET_BRONZE,
             f"{config.MANIFEST_PREFIX}/{config.DATASET_ID}.json", manifest)

    print(f">> Manifeste : bronze/{config.MANIFEST_PREFIX}/{config.DATASET_ID}.json "
          f"({len(all_files)} fichiers cumules, {len(kept)} nouveaux/enregistres "
          f"({healed} gueris), {skipped} ignores, annees {manifest['trip_years']})")
    if not kept:
        print("[!] Rien de nouveau a ingerer (deja en bronze).")


if __name__ == "__main__":
    main()
