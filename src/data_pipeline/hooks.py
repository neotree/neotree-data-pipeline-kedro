"""Project hooks."""
from typing import Any, Dict, Iterable, Optional

from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.versioning import Journal
from data_pipeline.pipelines import data_engineering as de



class ProjectHooks:
    @hook_impl
    def register_pipelines(self):
        """Register the project's pipeline.

        Returns:
            A mapping from a pipeline name to a ``Pipeline`` object.

        """
        data_engineering_pipeline = de.create_pipeline();

        return {
            "de": data_engineering_pipeline,
            "__default__": data_engineering_pipeline
        }

    @hook_impl
    def register_config_loader(self, conf_paths: Iterable[str]):
        return ConfigLoader(conf_paths)
  
    @hook_impl
    def register_catalog(self,catalog):
        return DataCatalog.from_config(
            catalog
        )