from dagster_dbt import DagsterDbtTranslator
from dagster import AssetKey


class MultiProjectDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        fqn = dbt_resource_props["fqn"]

        if len(fqn) >= 1:
            project_name = fqn[0]
            clean_project = project_name.replace("_dbt", "")
            return clean_project

        return "default"

    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        fqn = dbt_resource_props["fqn"]

        project_name = fqn[0].replace("_dbt", "")
        layer = fqn[1] if len(fqn) >= 2 else "default"

        if resource_type == "source":
            return AssetKey([project_name, layer, f"raw_{name}"])

        return AssetKey([project_name, layer, name])
