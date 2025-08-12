from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path
import re

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def _safe_table(name: str) -> str:
    # lower, non-alnum -> _, trim/strip
    s = re.sub(r'[^a-z0-9_]', '_', str(name).lower()).strip('_')
    return s[:128] or 'table'


@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    """
    Expects:
        data = {
            "datetime_dim": DataFrame,
            "passenger_count_dim": DataFrame,
            "trip_distance_dim": DataFrame,
            "rate_code_dim": DataFrame,
            "pickup_location_dim": DataFrame,
            "dropoff_location_dim": DataFrame,
            "payment_type_dim": DataFrame,
            "fact_table": DataFrame,
        }
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    project_id = 'silken-forest-466023-a2'
    dataset_id = 'uber_data_engineering'

    writer = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))

    # default behavior; override with exporter block kwargs if needed
    default_if_exists = kwargs.get('if_exists', 'replace')  # 'append'|'replace'|'fail'

    for key, value in (data or {}).items():
        if not isinstance(value, DataFrame):
            # Coerce only if it looks like a tabular structure; else fail fast
            try:
                value = DataFrame(value)
            except Exception as e:
                raise TypeError(f"Output '{key}' is not a DataFrame.") from e

        table = _safe_table(key)
        table_id = f"{project_id}.{dataset_id}.{table}"

        # Example policy: dims append, fact replace (tweak to your preference)
        if_exists = default_if_exists
        if key.endswith('_dim'):
            if_exists = kwargs.get('dim_if_exists', 'append')
        elif key == 'fact_table':
            if_exists = kwargs.get('fact_if_exists', 'replace')

        writer.export(
            value,
            table_id,
            if_exists=if_exists,
        )
        print(f"Wrote {len(value):,} rows -> {table_id} (if_exists={if_exists})")
