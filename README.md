# ETL Scraper — Vehicle Listings

ETL pipeline that scrapes vehicle listings from multiple sources in Colombia, transforms the data into Parquet files, and uploads them to S3 for analytics via AWS Athena.

## Sources

| Source | Endpoint | Daily pages | Schedule (Colombia) |
|---|---|---|---|
| MercadoLibre | `POST /meli/product` | 3 | 10:30 PM |
| Carroya | `POST /carroya/vehiculos` | 10 | 10:40 PM |
| Usados Renting | `POST /usados-renting/vehiculos` | 5 | 10:55 PM |
| VendeTuNave | `POST /vendetunave/vehiculos` | all | 11:10 PM |

## Architecture

```
EventBridge (4 rules, one per source)
  └── Lambda: etl-scraper
        ├── extract.py   →  POST to scraper API → returns JSON
        ├── transform.py →  JSON → pandas DataFrame (homologated schema)
        └── load.py      →  DataFrame → Parquet → S3 (scraper-meli bucket)
```

Each source is invoked independently via EventBridge with `{"source": "<name>"}` so each Lambda has its own 15-min timeout.

### AWS Resources
- **scraper API** — Lambda URL that scrapes all sources and returns raw JSON
- **etl-scraper** — Lambda that runs the ETL daily, deployed via ECR image
- **scraper-meli** — S3 bucket with Parquet files
- **scraper_meli** — Glue database + Athena workgroup for querying

## S3 Layout

```
scraper-meli/
  carros/data_YYYY-MM-DD_<source>.parquet   # daily loads
  initial_load/data_YYYY-MM-DD_<source>_p<N>.parquet  # initial loads (paginated)
  athena-results/                            # Athena query results
```

## Unified Schema

All sources are homologated to the same 35-column schema:

`product`, `price`, `link`, `year`, `linea`, `description`, `vehicle_brand`, `vehicle_line`, `color`, `body_type`, `fuel_type`, `engine`, `transmission`, `version`, `id`, `years`, `mileage`, `last_plate_digit`, `plate_parity`, `location_city2`, `location_city`, `sku`, `image_url`, `item_condition`, `horsepower`, `traction_control`, `steering`, `single_owner`, `negotiable_price`, `num_doors`, `seating_capacity`, `json_ld_extra`, `specs_extra`, `_created`, `source`

## Athena

Database: `scraper_meli`  
Workgroup: `etl-scraper`  
Main view: `carros` (union of `carros_daily` + `carros_initial`)

## Local Development

```bash
# Setup
cp .env.example .env  # fill in your values
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Run daily ETL locally (one source)
cd src && python main.py carros           # meli
cd src && python -c "from main import main_carroya; main_carroya()"
cd src && python -c "from main import main_usados_renting; main_usados_renting()"
cd src && python -c "from main import main_vendetunave; main_vendetunave()"

# Run initial load (paginated, with S3 verification)
python src/initial_load.py --source carroya --carroya-start-page 1
python src/initial_load.py --source usados_renting --usados-renting-start-page 1
python src/initial_load.py --source vendetunave --vendetunave-start-page 1
python src/initial_load.py --source all  # all sources from page 1
```

## Infrastructure

Managed with Terraform in `infra/`. Requires `infra/terraform.tfvars` (not committed):

```bash
cp infra/terraform.tfvars.example infra/terraform.tfvars  # fill in your values
cd infra
terraform init
terraform apply
```

## CI/CD

`.github/workflows/deploy-lambda.yml` — triggers on push to `master`, builds a Docker image, pushes to ECR, and updates the Lambda function code.

Requires GitHub secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
