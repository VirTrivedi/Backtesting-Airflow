# Backtesting-Airflow

Automation for the market-data backtesting pipeline. An Airflow DAG turns a raw PCAP capture into per-symbol order books and uploads them to S3. A driver script starts the EC2 box that runs Airflow, triggers the DAG, waits for it to finish, and stops the instance again so you only pay for the compute you use.

## How it works

```
start_backtesting.py (run locally / from cron)
  1. start the EC2 instance, look up its public IP
  2. wait for Airflow's health endpoint
  3. get a JWT and trigger the `backtesting_pipeline` DAG
  4. poll the DAG run until success / failure (up to 120 min)
  5. stop the EC2 instance (also on failure, timeout, or Ctrl-C)

backtesting_pipeline DAG (on the EC2 instance)
  download_pcap_from_s3 -> check_system_resources -> process_pcap_file
    -> get_histbook_file_list -> process_single_book_events_file (mapped per file)
    -> collect_histbook_results -> upload_results_to_s3_sync
```

| Task | What it does |
|---|---|
| `download_pcap_from_s3` | Picks the most recently modified `.pcap` / `.pcap.zst` in the bucket root and downloads it to `/tmp`. Parses `EXCHANGE.VENUE.LETTER-DATEINT.COLO` from the filename to get venue, date and colo. |
| `check_system_resources` | Logs disk, memory, CPU count, and checks that the `PcapToRef` and `HistBook` binaries exist. |
| `process_pcap_file` | Runs `/home/airflow/PcapToRef` with the output directory, venue, date, colo, and a UTC start and end hour of 0 and 23 (`-o -v -d -c -s 0 -e 23`) and collects the `.bin` book-event files it writes under `/tmp/<dateint>/<venue>/`. |
| `process_single_book_events_file` | Dynamically mapped: one `/home/airflow/HistBook -i <file> -o /tmp/<dateint>/<venue>/books/` run per book-events file. |
| `collect_histbook_results` | Counts successful and failed HistBook runs. |
| `upload_results_to_s3_sync` | `aws s3 sync` of the output to `s3://<bucket>/<dateint>/<venue>/`, then deletes the local PCAP and working directory. |

On any task failure `cleanup_on_failure` removes the downloaded PCAP and the `/tmp/<8-digit-date>` directories. The DAG has `schedule=None`, so it runs when triggered.

`PcapToRef` and `HistBook` are the binaries built by the sibling `pcaptoref` and `histbook` projects.

## Files

| File | Purpose |
|---|---|
| `backtesting_pipeline.py` | The Airflow DAG (copy into the instance's `dags/` folder). |
| `start_backtesting.py` | EC2 start, DAG trigger, wait, EC2 stop. Calls the Airflow 3 REST API (`/auth/token` for a JWT, then `/api/v2/...`). |
| `run_backtesting.sh` | Loads `.env`, sets up the Python path, and runs `start_backtesting.py`. This is the entry point for cron. |
| `airflow-setup.yml` | Ansible playbook that provisions Airflow plus PostgreSQL on a Debian/Ubuntu host (apt): `airflow` user, virtualenv at `/home/airflow/airflow-env`, database, and systemd units for the webserver (port 8080) and scheduler. It installs Airflow 2.10.3 (`airflow_version`). |
| `inventory` | Ansible inventory (localhost). |
| `test-airflow.py` | Small `hello_world` DAG that also lists S3 buckets, for checking Airflow and AWS credentials. |
| `requirements.txt` | `boto3`, `requests` for the driver script. |

## Setup

1. **Provision the instance.** Set `airflow_password` and `airflow_admin_password` in `airflow-setup.yml`, then:
   ```bash
   ansible-playbook -i inventory airflow-setup.yml
   ```
2. **Install the binaries and DAG.** Put `PcapToRef` and `HistBook` in `/home/airflow/`, and copy `backtesting_pipeline.py` into `$AIRFLOW_HOME/dags/` (the playbook leaves the DAGs folder at its default). The DAG imports `boto3` and shells out to `aws s3 sync`, so `boto3` must be installed in the Airflow virtualenv and the AWS CLI on the host, with credentials (for example an instance role) that can read and write the bucket. The playbook does not install either.
3. **Configure the driver.** The driver uses the `/api/v2` endpoints, which are Airflow 3's API, so the instance has to run Airflow 3 for it to work.

   `pip install -r requirements.txt`, then fill in `.env`:

   | Variable | Required | Default |
   |---|---|---|
   | `EC2_INSTANCE_ID`, `EC2_REGION` | yes |  |
   | `AIRFLOW_BASE_URL`, `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` | yes |  |
   | `DAG_ID` | no | `backtesting_pipeline` |
   | `EC2_START_TIMEOUT_MINUTES` | no | `10` |
   | `AIRFLOW_WAIT_TIMEOUT_MINUTES` | no | `6` |
   | `SERVICE_INIT_WAIT_SECONDS` | no | `60` |

   The public IP changes each time the instance starts, so the script rebuilds the URL from the current IP and keeps only the port from `AIRFLOW_BASE_URL`.

## Usage

```bash
./run_backtesting.sh                      # uses .env
```

1. Upload a PCAP named `EXCHANGE.VENUE.LETTER-DATEINT.COLO.pcap.zst` (for example, with date `20220801`) to the bucket root.
2. Run the script. It prints a link to the DAG's grid view once the run starts.

Results land in `s3://vir-airflow/<dateint>/<venue>/`.
