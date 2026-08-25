# DataLake — NYC Taxi × Météo : pipeline médallion en une commande

Pipeline de données sur les fichiers TLC parquet déjà présents dans `data/` (yellow 2009 au
schéma legacy, yellow/green/fhv 2016 au schéma moderne, ~7,8 Go) : ingestion du brut tel quel
dans un stockage objet, réconciliation des schémas avec Spark sur un vrai master
(`spark://spark-master:7077`), agrégats métier, puis dashboard HTML statique (français, ECharts).

## Architecture

```
 data/<year>/<month>/<vehicle>_tripdata_<year>-<month>.parquet   (brut local, inchangé)
   |
   | make ingest          copie TELLE QUELLE, idempotente
   v
 bronze/<vehicle_type>/<year>/<month>/...parquet     (MinIO)  + manifeste _manifest/nyc-taxi-trips.json
 bronze/weather/<year>/<fichier>.json                (stub, exports manuels)
   |
   | make bronze-to-silver   schémas legacy + modernes -> modèle canonique unique,
   |                         GPS et LocationID côte à côte, suppléments absents = NULL,
   v                         horodatages corrompus écartés
 silver/nyc-taxi-trips/trips    Parquet partitionné vehicle_type/year/month
   |
   | make silver-to-gold
   v
 gold/nyc-taxi-trips/*.json   surcharge_evolution · zone_flows · price_per_km_by_zone
                              activity_heatmap · weather_deviation · kpis · meta
   |
   | make export && make site
   v
 dashboard/site/index.html    dashboard statique ECharts (chord, ridgeline, heatmap…)
```

## Démarrage

Prérequis : [Docker Desktop](https://www.docker.com/products/docker-desktop/) démarré et GNU Make.

```bash
make run              # dataset complet (~7,8 Go)
make run SAMPLE=3     # rapide : 3 premiers fichiers seulement
make smoke            # test d'intégrité du cluster (Spark master + MinIO roundtrip)
```

| Sortie | Emplacement |
|---|---|
| Dashboard | `dashboard/site/index.html` |
| Console MinIO | http://localhost:9001 (`minioadmin` / `minioadmin123`) |
| UI Spark master | http://localhost:18080 |

## Cibles make

| Cible | Rôle |
|---|---|
| `up` | démarre MinIO + buckets + Spark master/worker |
| `ingest` | pousse les parquets vers bronze (`SAMPLE=N` pour limiter à N fichiers) |
| `weather` | stub : pousse `data/weather/*.json` vers bronze/weather/ |
| `bronze-to-silver` | réconciliation des schémas → Parquet partitionné |
| `silver-to-gold` | les 5 analyses du notebook → marts JSON |
| `export` / `site` | export gold → `dashboard/site/data/`, rendu Jinja2 → `index.html` |
| `run` | tout l'enchaînement ci-dessus |
| `down` / `logs` / `clean` | arrêt, logs, arrêt + purge du volume MinIO |

## Test sans Docker (LAKE=local)

Avec `pyspark` 3.5.x et Java 17 installés localement :

```powershell
$env:LAKE="local"; $env:SPARK_MASTER_URL="local[*]"; $env:SAMPLE="3"
python pipeline/stages/ingest_trips.py --sample 3
python pipeline/stages/bronze_to_silver.py
python pipeline/stages/silver_to_gold.py
python pipeline/stages/export_gold.py
python scripts/render_site.py
```

Tout est écrit sous `lake_local\` (même arborescence bronze/silver/gold), sans MinIO ni conteneur.

## Déploiement Vercel

Le dashboard est 100 % statique. Le `vercel.json` racine sert `dashboard/site`
(`outputDirectory` + `cleanUrls`), et le `.vercelignore` exclut `data/`,
`lake_local/` et les caches. Après un run complet :

```bash
npx vercel --prod        # → https://nyc-taxi-xi.vercel.app/
```

En déploiement Git, `dashboard/site/` n'est **pas** ignoré : un simple push
suffit après chaque `make site`.

## Points de conception

- **Convention bronze** `bronze/<type>/<année>/<mois>/<fichier>` : répond sans ouvrir les fichiers
  aux questions « quels types ingérés », « quelle période couverte », « ce mois est-il déjà là ».
- **Drift de schéma** : cartes de renommage legacy (2009 : `Fare_Amt`, `Start_Lon`…) et moderne
  (`tpep_`/`lpep_`, `PULocationID`, fhv `pickup_datetime`…) vers un schéma canonique ; les
  suppléments introduits plus tard (`improvement_surcharge`, `congestion_surcharge`,
  `airport_fee`, `cbd_convenience_fee`) sont NULL quand le fichier source ne les contient pas —
  jamais coalescés à 0.
- **Localisation** : coordonnées GPS (avant 07/2016) et identifiants de zone conservés en colonnes
  distinctes ; les analyses zonées n'utilisent que les courses portant des LocationID.
- Ne modifie jamais le notebook ; ne télécharge aucune donnée.
