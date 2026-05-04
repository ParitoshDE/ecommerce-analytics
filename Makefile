.PHONY: download upload spark bq-load dbt-deps dbt-run dbt-test all tf-init tf-apply

download:
	python scripts/download_data.py

upload:
	python scripts/upload_to_gcs.py

spark:
	python spark/transform_events.py

bq-load:
	python scripts/load_to_bigquery.py

dbt-deps:
	cd dbt && dbt deps --project-dir . --profiles-dir .

dbt-run:
	cd dbt && dbt deps --project-dir . --profiles-dir . && dbt run --project-dir . --profiles-dir .

dbt-test:
	cd dbt && dbt test --project-dir . --profiles-dir .

test:
	pytest tests/ -v

all: download upload spark bq-load dbt-run dbt-test

tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && terraform plan

tf-apply:
	cd terraform && terraform apply -auto-approve

tf-destroy:
	cd terraform && terraform destroy
