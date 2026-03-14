# ETL Scraper — MercadoLibre Vehicles

ETL pipeline that scrapes vehicle listings (cars/motos) from MercadoLibre Colombia, transforms the data into Parquet files, and uploads them to S3 for analytics via AWS Athena.

## Architecture

```
EventBridge (daily 3 AM UTC)
  └── Lambda: etl-scraper
        ├── extract.py   →  POST to meli-scrapper Lambda → returns JSON
        ├── transform.py →  JSON → pandas DataFrame (cleans types, locations, extracts IDs)
        └── load.py      →  DataFrame → Parquet → S3 (scraper-meli bucket)
```

### AWS Resources
- **meli-scrapper** — Lambda that scrapes MercadoLibre and returns raw JSON
- **etl-scraper** — Lambda that runs the ETL daily, deployed via ECR image
- **scraper-meli** — S3 bucket with Parquet files
- **scraper_meli** — Glue database + Athena workgroup for querying

## S3 Layout

```
scraper-meli/
  carros/data_YYYY-MM-DD.parquet    # daily loads
  initial_load/data_2026-02-18.parquet
  athena-results/                   # Athena query results
```

## Athena

Database: `scraper_meli`
Workgroup: `etl-scraper`
Main view: `carros` (union of `carros_daily` + `carros_initial`)

## Local Development

```bash
# Setup
uv venv
cat >> .venv/bin/activate << 'EOF'

if ! [ -z "${PYTHONPATH+_}" ] ; then
    _OLD_VIRTUAL_PYTHONPATH="$PYTHONPATH"
fi
PYTHONPATH="$(dirname "$VIRTUAL_ENV")"
export PYTHONPATH
EOF
source .venv/bin/activate
uv pip install -r requirements.txt

# Run ETL locally
python src/main.py Carros
python src/main.py Motos

# Run initial historical load
python src/initial_load.py
```

## Infrastructure

Managed with Terraform in `infra/`. Requires `infra/terraform.tfvars` (not committed):

```hcl
scraper_api_url = "https://<meli-scrapper-url>.lambda-url.us-east-1.on.aws"
api_key         = "<api-key>"
```

```bash
cd infra
terraform init
terraform apply
```

## CI/CD

`.github/workflows/deploy-lambda.yml` — triggers on push to `master`, builds a Docker image, pushes to ECR, and updates the Lambda function code.

Requires GitHub secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
