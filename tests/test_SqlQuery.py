import unittest
from src.xquery.lib.db.db_connections import SqlServerConnection
from src.xquery.lib.query.queries import SqlQuery
from src.xquery.lib.config import DbState
from tests.tests_config import valid_config
from pandas import DataFrame


class TestSqlQuery(unittest.TestCase):
    def setUp(self):
        """Setup data needed to carry out tests"""
        self.sql_cnxn = SqlServerConnection(valid_config)
        self.session = self.sql_cnxn.create_session()
        self.sql_query = SqlQuery(self.session)

    def test_if_connected(self):
        self.assertEqual(DbState.CONNECTED, self.sql_cnxn.get_connection_state())

    def test_execute_query_create_df(self):
        df = (
            self.sql_query.execute_query_create_df(
                'SELECT 1 AS test'
            )
        )
        test_df = DataFrame({'test': [1]})
        self.assertIsInstance(df, DataFrame)
        self.assertTrue(DataFrame.equals(df, test_df))


