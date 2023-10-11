# ELT - Show MM - Data warehouse

## Getting Started

**Developement - Installation via `requirements.txt`**:

```shell
python3 -m venv myenv
source myenv/bin/activate
pip3 install -r requirements.txt
```

**Production - Installation via `requirements.txt`**:

```shell
pip3 install --target=./etl_python -r requirements.txt
# Crontab
0 */6 * * * PYTHONPATH=/home/quangdn3/apps/etl-show-mm/etl_python python3 /home/quangdn3/apps/etl-show-mm/app/import.py -env /home/quangdn3/apps/etl-show-mm/.env
15 */2 * * * * PYTHONPATH=/home/quangdn3/apps/etl-show-mm/etl_python python3 /home/quangdn3/apps/etl-show-mm/app/main.py -env /home/quangdn3/apps/etl-show-mm/.env -a dim
45 */2 * * * * PYTHONPATH=/home/quangdn3/apps/etl-show-mm/etl_python python3 /home/quangdn3/apps/etl-show-mm/app/main.py -env /home/quangdn3/apps/etl-show-mm/.env -a daily

```

## Usage

Replace the values in **.env.example** with your values and rename this file to **.env**:

*Remember never to commit secrets saved in .env files to Github.*

## Setup

```sh
# Import logs daily
python3 app/import.py -env ./.env

# Run transform & load daily
python3 app/main.py -env ./.env -a dim -s 2022-09-28
python3 app/main.py -env ./.env -a dim -s 2022-09-28 -e 2022-09-30

python3 app/main.py -env ./.env -a dim_update -s 2022-09-28

python3 app/main.py -env ./.env -a daily -s 2022-09-28
python3 app/main.py -env ./.env -a daily -s 2022-09-20 -e 2022-09-30

```

## Download Logs

```bash
scp -J quangdn3@10.30.96.26 quangdn3@10.30.42.37:/gamelog/show2/realtime/show_dev/\* ./log_files

scp -r -J quangdn3@10.30.96.26 ./app/* quangdn3@10.30.42.37:/home/quangdn3/apps/etl-show-mm/app/

scp -r -J quangdn3@10.30.96.26 ./dags/* quangdn3@10.30.42.37:/home/quangdn3/docker/airflow/dags/
```
