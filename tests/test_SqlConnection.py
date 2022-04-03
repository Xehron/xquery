from src.xquery.lib.db.db_connections import SqlServerConnection
import unittest
import pyodbc
from src.xquery.lib.config import DbState, DbType
from tests.tests_config import valid_config


#  WARNING: tests are carried out with use of real db connection instead of mock.
class TestSqlConnection(unittest.TestCase):
    def setUp(self):
        """Setup data needed to carry out tests"""
        self.sql_cnxn = SqlServerConnection(valid_config)
        self.valid_session = self.sql_cnxn.create_session()

    def test_create_session_correct_config_return_value(self):
        """Check if connection is returned"""
        self.assertIsInstance(self.valid_session, pyodbc.Connection)

    def test_create_session_correct_config_state(self):
        """Check state after connection with valid params"""
        self.assertEqual(DbState.CONNECTED, self.sql_cnxn.connection_state)

    def test_get_session_id_returns_int(self):
        """Check state after connection with valid params"""
        self.assertIsInstance(self.sql_cnxn.get_session_id(), int)

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
        self.assertEqual(DbType.SQL_SERVER, self.sql_cnxn.get_db_type())
