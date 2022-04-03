from src.xquery.lib.db.db_connections import BqServerConnection
import unittest
from src.xquery.lib.config import DbState, DbType, BQ_CLIENT_SECRET_FILE
from google.cloud import bigquery


#  WARNING: tests are carried out with use of real db connection instead of mock.
class TestSqlConnection(unittest.TestCase):
    def setUp(self):
        """Setup data needed to carry out tests"""
        self.sql_cnxn = BqServerConnection(BQ_CLIENT_SECRET_FILE)
        self.valid_session = self.sql_cnxn.create_session()

    def test_create_session_correct_config_return_value(self):
        """Check if connection is returned"""
        self.assertIsInstance(self.valid_session, bigquery.client.Client)

    def test_create_session_correct_config_state(self):
        """Check state after connection with valid params"""
        self.assertEqual(DbState.CONNECTED, self.sql_cnxn.connection_state)

    def test_close_connection(self):
        """Check state after connection with valid params"""
        self.sql_cnxn.close_connection()
        self.assertEqual(DbState.DISCONNECTED, self.sql_cnxn.connection_state)
        self.assertIsInstance(self.sql_cnxn.session, type(None))

    def test_get_connection_state(self):
        """Check state after connection with valid params"""
        self.sql_cnxn.connection_state = DbState.DISCONNECTED
        self.assertEqual(DbState.DISCONNECTED, self.sql_cnxn.get_connection_state())

    def test_set_connection_state(self):
        """Check state after connection with valid params"""
        self.sql_cnxn.set_connection_state(DbState.DISCONNECTED)
        self.assertEqual(DbState.DISCONNECTED, self.sql_cnxn.get_connection_state())

    def test_get_db_type(self):
        self.assertEqual(DbType.BIGQUERY, self.sql_cnxn.get_db_type())
