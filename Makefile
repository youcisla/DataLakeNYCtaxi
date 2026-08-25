# ---------------------------------------------------------------------------
#  DataLake - NYC Taxi x Meteo : pipeline complet en une seule commande
#
#  Usage :
#    make all                          # TOUT : stack + ingest + meteo + silver
#                                      # + gold + dashboard, puis deploy Vercel
#    make run                          # comme `all` sans le deploy Vercel
#    make run SAMPLE=3                 # rapide : 3 premiers fichiers seulement
#    make ingest SAMPLE=3              # ingestion seule, echantillonnee
#    make weather                      # meteo seule (mois manquants uniquement)
#    make smoke                        # verifie le cluster Spark master + MinIO
#    make down                         # arrete la stack
#    make logs                         # suit les logs
#    make clean                        # arrete ET purge les donnees (volume MinIO)
#
#  Nouveaux fichiers : depose-les dans data/ puis relance `make all`.
#  L'ingestion est idempotente (ce qui existe deja en bronze est saute), le
#  manifeste se repare tout seul apres un run interrompu, et l'etape meteo
#  n'est pas bloquante (l'API Open-Meteo a ~5 jours de retard sur le mois
#  en cours : un echec meteo n'arrete pas le pipeline).
# ---------------------------------------------------------------------------

DATA_PATH ?= data
SAMPLE    ?= 0
COMPOSE    = docker compose

SAMPLE_FLAG = $(if $(filter-out 0,$(SAMPLE)),--sample $(SAMPLE),)

# Rend toutes les variables make visibles par docker compose (${DATA_PATH}, ${SAMPLE}...)
.EXPORT_ALL_VARIABLES:

.PHONY: all run deploy up ingest weather bronze-to-silver silver-to-gold \
        export site smoke down logs clean

up:
	@docker info --format ">> Docker OK ({{.ServerVersion}})" || (echo ">> Docker Desktop n'est pas demarre." && exit 1)
	$(COMPOSE) up -d --build minio minio-setup spark-master spark-worker

ingest:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/ingest_trips.py $(SAMPLE_FLAG)

# Non bloquant : pas de meteo pour les tous derniers mois (delai Open-Meteo),
# ce n'est pas une raison d'arreter le pipeline.
weather:
	-$(COMPOSE) run --rm pipeline python3 pipeline/stages/ingest_weather.py

bronze-to-silver:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/bronze_to_silver.py

silver-to-gold:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/silver_to_gold.py

export:
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/export_gold.py

site:
	$(COMPOSE) run --rm pipeline python3 scripts/render_site.py

deploy:
	npx vercel deploy --prod --yes

run: up ingest weather bronze-to-silver silver-to-gold export site
	@echo ">> Pipeline termine. Dashboard : dashboard/site/index.html"

all: run deploy
	@echo ">> Tout est en ligne : https://nyc-taxi-xi.vercel.app"

smoke: up
	$(COMPOSE) run --rm pipeline python3 pipeline/stages/smoke.py

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

clean:
	$(COMPOSE) down -v --remove-orphans
