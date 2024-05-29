# ETL for car web scraping project
this project use GithubActions to Extract Transform and Load data from web scraping to S3 ready to be used for Analytic projects

## 0. Setup
Please create your virtual environment before, for example
```bash
python3 -m venv myenv
source myenv/bin/activate
```
Then run
```bash
pip install -r requirements.txt
python src/initial_load.py
```
## 1. Initial load
in all Data engineering project, we need to realize first an initial load of our data. [Initial load](src/initial_load.py) has the needed code.

## 2. daily cron
[main.py](src/main.py) is the principal code that github actions run daily at 7:00, the source code is in [.github/workflows/send-data.yaml](.github/workflows/send-data.yml)

## 3. Final data
each day, we upload to S3 an CSV file with the processed data.

