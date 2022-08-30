import config
from datasources import ClientsDataSource, ItemsDataSource


def get_items_datasource() -> ItemsDataSource:
    return ItemsDataSource(config.ITEMS_DATASOURCE_FILEPATH)


def get_clients_datasource() -> ClientsDataSource:
    return ClientsDataSource(config.CLIENTS_DATASOURCE_FILEPATH)
