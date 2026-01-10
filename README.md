# CDC Pipeline - MongoDB/Postgres → ClickHouse

Pulls data from MongoDB and Postgres into ClickHouse, handles CDC, transforms with dbt. Built this because source collections don't have `updated_at` fields.

## Running it

```bash
docker-compose up -d
python setup/generate_data.py
# go to http://localhost:3001, hit "Materialize all"
```

Takes about 10 minutes first run.

## The problem

MongoDB `users` collection looks like this:
```json
{
  "_id": "...",
  "user_id": "USER00000001",
  "firstName": "Ada",
  "lastName": "Obi",
  "occupation": "Engineer",
  "state": "Lagos"
}
```

No timestamps. Can't do `WHERE updated_at > last_run`. So.

## How I dealt with it..

**MongoDB (no timestamps):** Full load daily → dbt snapshot handles change detection. Yes it moves 150K rows every time even if nothing changed. It's fine, takes 2 minutes.

**Postgres (has timestamps):** Normal incremental CDC. Query `WHERE updated_at > last_load`, done.

For the SCD Type 2 stuff on users, dbt snapshots do the heavy lifting:

```sql
{% snapshot users_snapshot %}
{{
    config(
      target_schema='data_pipeline',
      unique_key='_id',
      strategy='check',
      check_cols=['state', 'occupation']
    )
}}
select * from {{ ref('stg_users') }}
{% endsnapshot %}
```

10 lines instead of writing all that merge logic myself.

## Why not log-based CDC?

Looked into it. Would need Kafka, Zookeeper, Connect, schema registry... for a pipeline that's fine with 15-minute latency. Nah.

If you need sub-second CDC or you're doing fraud detection where you care about intermediate states, go for it. This isn't that.

## Architecture

```
MongoDB/Postgres → Dagster extracts → MinIO staging → ClickHouse raw → dbt models → marts
```

MinIO because I didn't want to pay for S3 during development.

## Testing CDC

```bash
# change something
docker exec source-mongodb mongosh ... -eval "
  db.users.updateOne({user_id: 'user_001'}, {\$set: {occupation: 'Senior Engineer'}})
"

# rerun
docker exec dagster_code_server dagster asset materialize --select raw_users dbt_assets

# check it worked
docker exec warehouse-clickhouse clickhouse-client --query "
  SELECT occupation, dbt_valid_from, dbt_valid_to 
  FROM data_pipeline.users_snapshot 
  WHERE user_id = 'user_001'
"
```

Should see two rows - old one with `dbt_valid_to` set, new one with NULL.

There's also `setup/simulate_cdc.py` that does a bunch of inserts/updates if you want to stress test.

## Ports

- Dagster: 3001
- ClickHouse: 8124
- MinIO: 9004
- Postgres: 5434
- Mongo: 27017

## CI

Just linting and dbt validation. No actual pipeline runs - didn't set up test databases in GitHub Actions. PRs welcome if you want to add that.

```bash
# locally
ruff check dagster_code/
black --check dagster_code/
cd dbt_project/dbt && dbt debug --profiles-dir .
```

## Stuff I'd add for prod

- Actual monitoring (Datadog or similar)
- Alerting on failures
- Partition the big tables
- Move secrets out of .env

## Project layout

```
dagster_code/
  assets/           # extraction logic
  clickhouse_load_tool/  # ELT helper
dbt_project/dbt/
  snapshots/        # SCD Type 2
  models/staging/   # cleaning
  models/marts/     # final tables
setup/              # data generation, init scripts
```