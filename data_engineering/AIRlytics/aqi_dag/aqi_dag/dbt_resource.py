import os
from dagster_dbt import DbtCliResource
from .constants import dbt_project_dir

dbt_resource = {
    "dbt": DbtCliResource(
        project_dir=dbt_project_dir,
        profiles_dir=os.path.expanduser("~/.dbt")                   
    )
}