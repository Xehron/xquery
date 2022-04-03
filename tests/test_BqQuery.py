import unittest
from src.xquery.lib.db.db_connections import BqServerConnection
from src.xquery.lib.query.queries import BigQuery
from src.xquery.lib.config import DbState
from src.xquery.lib.config import BQ_CLIENT_SECRET_FILE
from pandas import DataFrame


class TestBqQuery(unittest.TestCase):
    def setUp(self):
        """Setup data needed to carry out tests"""
        self.bq_cnxn = BqServerConnection(BQ_CLIENT_SECRET_FILE)
        self.session = self.bq_cnxn.create_session()
        self.bq_query = BigQuery(self.session)

    def test_if_connected(self):
        self.assertEqual(DbState.CONNECTED, self.bq_cnxn.get_connection_state())

    def test_execute_query_create_df(self):
        df = (
            self.bq_query.execute_query_create_df(
                'SELECT 1 AS test'
            )
        )
        test_df = DataFrame({'test': [1]})
        self.assertIsInstance(df, DataFrame)
        # self.assertTrue(DataFrame.equals(df, test_df))  # this method won't work because Google implements creating DataFrame differently (no support for datetime, uses numpy int64 vs pandas Int64 etc.)self.assertTrue(DataFrame.equals(df, test_df))  # this method won't work because Google implements creating DataFrame differently (no support for datetime, uses Int64 vs int64 etc.)
        self.assertEqual(len(df), len(test_df))
        self.assertEqual(len(df.columns), len(test_df.columns))



