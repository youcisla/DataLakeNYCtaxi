# ---------------------------------------------------------------------------
#  DataLake - NYC Taxi x Meteo : pipeline complet en une seule commande
#
#  Usage :
#    make run                          # dataset complet (~7,8 Go)
#    make run SAMPLE=3                 # rapide : 3 premiers fichiers seulement
#    make ingest SAMPLE=3              # ingestion seule, echantillonnee
#    make smoke                        # verifie le cluster Spark master + MinIO
#    make down                         # arrete la stack
#    make logs                         # suit les logs
#    make clean                        # arrete ET purge les donnees (volume MinIO)
# ---------------------------------------------------------------------------

DATA_PATH ?= data
SAMPLE    ?= 0
COMPOSE    = docker compose

SAMPLE_FLAG = $(if $(filter-out 0,$(SAMPLE)),--sample $(SAMPLE),)

# Rend toutes les variables make visibles par docker compose (${DATA_PATH}, ${SAMPLE}...)
.EXPORT_ALL_VARIABLES:

.PHONY: run up ingest weather bronze-to-silver silver-to-gold export site \
        smoke down logs clean

up:
	@docker info --format ">> Docker OK ({{.ServerVersion}})" || (echo ">> Docker Desktop n'est pas demarre." && exit 1)
	$(COMPOSE) up -d --build minio minio-setup spark-master spark-worker

ingest:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/ingest_trips.py $(SAMPLE_FLAG)

weather:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/ingest_weather.py

bronze-to-silver:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/bronze_to_silver.py

silver-to-gold:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/silver_to_gold.py

export:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/export_gold.py

site:
	$(COMPOSE) run --rm pipeline python3 scripts/render_site.py

run: up ingest weather bronze-to-silver silver-to-gold export site
	@echo ">> Pipeline termine. Dashboard : dashboard/site/index.html"

smoke: up
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/smoke.py

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

clean:
	$(COMPOSE) down -v --remove-orphans
