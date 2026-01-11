from dagster_dbt import dbt_assets, DbtCliResource
from dagster import AssetExecutionContext
from .dbt_translator import MultiProjectDbtTranslator


@dbt_assets(
    manifest="/opt/dagster/app/dbt_project/dbt/target/manifest.json",
    dagster_dbt_translator=MultiProjectDbtTranslator(),
    select="resource_type:snapshot",
)
def dbt_snapshots(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Starting dbt snapshot run...")
    yield from dbt.cli(["snapshot"], context=context).stream()
    context.log.info("Completed dbt snapshot run!")


@dbt_assets(
    manifest="/opt/dagster/app/dbt_project/dbt/target/manifest.json",
    dagster_dbt_translator=MultiProjectDbtTranslator(),
    select="resource_type:model",
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Starting dbt build (models + tests)...")
    yield from dbt.cli(["build"], context=context).stream()
    context.log.info("Completed dbt build successfully!")
