import yaml
from importlib import import_module
from kraken_api.base import TradesAPI
"""
Factory for creating TradesAPI instances.
"""
class TradesAPIFactory:
    @staticmethod
    def create(data_source: str, pairs: list[str], last_n_days: int = None) -> TradesAPI:
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)

        if data_source not in config['data_sources']:
            raise ValueError(f'Invalid data source: {data_source}')

        source_config = config['data_sources'][data_source]
        module_name, class_name = source_config['class'].rsplit('.', 1)
        module = import_module(module_name)
        api_class = getattr(module, class_name)

        # Handle additional parameters
        params = source_config.get('params', {})
        if 'last_n_days' in params:
            params['last_n_days'] = last_n_days

        return api_class(pairs=pairs, **params) 