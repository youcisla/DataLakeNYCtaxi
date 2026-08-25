"""Rend le dashboard statique (HTML) dans dashboard/site/ à partir des JSON exportés."""
import json
import sys
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from pipeline import config

ANALYSE_FILES = ("surcharge_evolution.json", "zone_flows.json",
                 "price_per_km_by_zone.json", "activity_heatmap.json",
                 "weather_deviation.json", "zone_stats.json", "timeline.json",
                 "zone_lookup.json")


def read_json(path: Path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main():
    site = Path(config.SITE_DIR)
    data_root = site / "data"
    if not data_root.exists():
        print(f"[!] Aucune donnee exportee dans {data_root}. Lance d'abord l'etape export.")
        sys.exit(1)

    datasets = []
    for ds_dir in sorted(p for p in data_root.iterdir() if p.is_dir()):
        meta_path = ds_dir / "meta.json"
        if not meta_path.exists():
            continue
        missing = [f for f in ANALYSE_FILES if not (ds_dir / f).exists()]
        if missing:
            print(f"[!] [{ds_dir.name}] gold incomplet, ignore (manque : {missing})")
            continue
        datasets.append({
            "id": ds_dir.name,
            "meta": read_json(meta_path),
            "kpis": read_json(ds_dir / "kpis.json"),
            "surcharges": read_json(ds_dir / "surcharge_evolution.json"),
            "flux_zones": read_json(ds_dir / "zone_flows.json"),
            "prix_km": read_json(ds_dir / "price_per_km_by_zone.json"),
            "heatmap": read_json(ds_dir / "activity_heatmap.json"),
            "meteo": read_json(ds_dir / "weather_deviation.json"),
            "zone_stats": read_json(ds_dir / "zone_stats.json"),
            "timeline": read_json(ds_dir / "timeline.json"),
            "zones": read_json(ds_dir / "zone_lookup.json"),
        })

    if not datasets:
        print("[!] Aucun dataset complet trouve pour le rendu.")
        sys.exit(1)

    payload = {
        "genere_le": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "spark_master": config.SPARK_MASTER_URL,
        "datasets": datasets,
    }

    env = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"),
                      keep_trailing_newline=True)
    html = env.get_template("dashboard.html.j2").render(
        payload=payload,
        payload_json=json.dumps(payload, ensure_ascii=False),
    )
    out = site / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f">> Dashboard ecrit : {out}")


if __name__ == "__main__":
    main()
