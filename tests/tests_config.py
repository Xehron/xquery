from src.xquery.lib.config import SqlServerConfig

INVALID_BQ_CLIENT_SECRET_FILE = 'C:/credentials/invalid_bq_credentials.json'
valid_config = SqlServerConfig(server_name=r'server_name', database_name='data')
invalid_config = SqlServerConfig(server_name="INVALID_SERVER_NAME", database_name='TEST')
