import os
import re
import subprocess

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, String, Integer, Float, MetaData, Table, inspect, select, func, Column
from sqlalchemy_utils import database_exists, create_database

from pilotscope.Common.Index import Index
from pilotscope.Common.SSHConnector import SSHConnector
from pilotscope.DBController.BaseDBController import BaseDBController
from pilotscope.Exception.Exception import DBStatementTimeoutException, DatabaseCrashException, \
    PilotScopeInternalError, PilotScopeExecCommandException
from pilotscope.PilotConfig import MySQLConfig


# noinspection PyProtectedMember
class MySQLController(BaseDBController):
    _instances = set()

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        cls._instances.add(instance)
        return instance

    def __del__(self):
        self._disconnect()
        type(self)._instances.remove(self)

    def __init__(self, config: MySQLConfig, echo=True, enable_simulate_index=False):
        super().__init__(config, echo)
        self.config: MySQLConfig = config


    def _create_conn_str(self):
        return "{}://{}:{}@{}:{}/{}".format("mysql+pymysql", self.config.db_user, self.config.db_user_pwd,
                                               self.config.db_host,
                                               self.config.db_port, self.config.db)

    def _create_engine(self):
        """
        Create the database engine.

        :return: The created database engine.
        """
        conn_str = self._create_conn_str()

        if not database_exists(conn_str):
            create_database(conn_str, encoding="utf8", template="template0")

        return create_engine(conn_str, echo=self.echo, pool_size=10, pool_recycle=3600,
                             isolation_level="AUTOCOMMIT")

    def explain_physical_plan(self, sql, comment=""):
        """
        Get the physical plan from database's optimizer of a SQL query.

        :param sql: The SQL query to be explained.
        :param comment: A SQL comment will be added to the beginning of the SQL query.
        :return: The physical plan of the SQL query.
        """
        return self._explain(sql, comment, False)

    def _explain(self, sql, comment, execute: bool):
        return self.execute(text(self.get_explain_sql(sql)), True)[0][0]

    def get_explain_sql(self, sql):
        """
        Constructs an EXPLAIN SQL statement for a given SQL query.

        :param sql: The SQL query to explain.
        :param execute: A boolean flag indicating whether to execute the query plan.
        :param comment:  A SQL comment will be added to the beginning of the SQL query.
        :return: The result of executing the `EXPLAIN` SQL statement.
        """
        return "EXPLAIN format=json {}".format(sql)
        
    def explain_execution_plan(self, sql, comment=""):
        """
        Get the execution plan from database's optimizer of a SQL query.

        :param sql: The SQL query to be explained.
        :param comment: A SQL comment will be added to the beginning of the SQL query.
        :return: The execution plan of the SQL query.
        """
        pass

    def get_estimated_cost(self, sql, comment=""):
        """
        Get an estimated cost of a SQL query.

        :param sql: The SQL query for which to estimate the cost.
        :param comment:  A SQL comment will be added to the beginning of the SQL query.
        :return: The estimated total cost of executing the SQL query.
        """
        pass


    # def explain_physical_plan(self, sql):
    #     """
    #     Get a physical plan from database's optimizer for a given SQL query.

    #     :param sql: The SQL query to be explained.
    #     """
    #     pass

    # def explain_execution_plan(self, sql):
    #     """
    #     Get an execution plan from database's optimizer for a given SQL query.

    #     :param sql: The SQL query to be explained.
    #     """
    #     pass

    def execute(self, sql, fetch=False, fetch_column_name=False):
        """
        Execute a SQL query.

        :param sql: the SQL query to execute
        :param fetch: it indicates whether to fetch the result of the query
        :param fetch_column_name: it indicates whether to fetch the column names of the result.
        :return: the result of the query if fetch is True, otherwise None
        """
        row = None
        try:
            self._connect_if_loss()
            conn = self._get_connection()
            result = conn.execute(text(sql) if isinstance(sql, str) else sql)
            if fetch:
                row = result.all()
                if fetch_column_name:
                    row = [tuple(result.keys()), *row]
        except OperationalError as e:
            if "canceling statement due to statement timeout" in str(e):
                raise DBStatementTimeoutException(str(e))
            else:
                raise e
        except Exception as e:
            if "PilotScopePullEnd" not in str(e):
                raise e
        return row

    def set_hint(self, key, value):

        """
        Set the value of each hint (i.e., the run-time config) when execute SQL queries.
        The hints can be used to control the behavior of the database system in a session.

        For PostgreSQL, you can find all valid hints in https://www.postgresql.org/docs/13/runtime-config.html.

        For Spark, you can find all valid hints (called conf in Spark) in https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration

        :param key: The key associated with the hint.
        :param value: The value of the hint to be set.
        """
        raise NotImplementedError

    def create_index(self, index: Index):
        """
        Create an index on columns `index.columns` of table `index.table` with name `index.index_name`.

        :param index: a Index object including the information of the index
        """
        pass

    def drop_index(self, index: Index):
        """
        Drop an index by its index name.

        :param index: an index that will be dropped
        """
        pass

    def drop_all_indexes(self):
        """
        Drop all indexes across all tables in the database.
        This will not delete the system indexes and unique indexes.
        """
        pass

    def get_all_indexes_byte(self):
        """
        Get the size of all indexes across all tables in the database in bytes.
        This will include the system indexes and unique indexes.

        :return: the size of all indexes in bytes
        """
        pass

    def get_table_indexes_byte(self, table_name):
        """
        Get the size of all indexes on a table in bytes.
        This will include the system indexes and unique indexes.


        :param table_name: a table name that the indexes belong to
        :return: the size of all indexes on the table in bytes
        """
        pass

    def get_index_byte(self, index: Index):
        """
        Get the size of an index in bytes by its index name.

        :param index: the index to get size
        :return: the size of the index in bytes
        """
        pass

if __name__ == "__main__":
    controller = MySQLController(MySQLConfig())
    controller._connect_if_loss()
    controller.execute("SELECT * FROM test_table")
    controller.close()