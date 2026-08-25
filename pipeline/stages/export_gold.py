"""Export : copie les agregats gold vers dashboard/site/data/ pour le dashboard statique.
Spark n'ecrit jamais directement sur le systeme hote."""
from pathlib import Path

from pipeline import config
from pipeline.utils_s3 import client, list_keys, load_manifests


def main():
    s3 = client()
    data_root = Path(config.SITE_DIR) / "data"
    data_root.mkdir(parents=True, exist_ok=True)

    for man in load_manifests(s3):
        if man.get("kind") != "trips":
            continue
        ds = man["dataset_id"]
        dest = data_root / ds
        dest.mkdir(parents=True, exist_ok=True)
        print(f">> export gold/{ds}/ -> {dest}")

        for key in list_keys(s3, config.BUCKET_GOLD, f"{ds}/"):
            target = dest / Path(key).name
            s3.download_file(config.BUCKET_GOLD, key, str(target))
            print(f"   - {target.name}")
        print(f">> Export [{ds}] termine.")


if __name__ == "__main__":
    main()
