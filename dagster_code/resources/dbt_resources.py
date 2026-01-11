from dagster_dbt import DbtCliResource

dbt = DbtCliResource(
    project_dir="/opt/dagster/app/dbt_project/dbt",
    profiles_dir="/opt/dagster/app/dbt_project/dbt",
)
