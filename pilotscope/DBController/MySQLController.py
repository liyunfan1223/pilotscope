import os
import re
import pymysql
import subprocess

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, String, Text, Integer, Float, MetaData, Table, inspect, select, func, Column
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
        self.execute("SET @@explain_json_format_version = 2;")
        return self.execute(text(self.get_explain_sql(sql)), True)[0][0]

    def get_possible_keys(self, sql, comment=""):
        possible_keys = dict()
        outputs = self.execute(text(self.get_explain_sql(sql, format=None)), True)
        TABLE_IDX = 2
        POSSIBLE_KEYS_IDX = 5
        for output in outputs:
            if output[POSSIBLE_KEYS_IDX] is not None:
                possible_keys[output[TABLE_IDX]] = [x for x in output[POSSIBLE_KEYS_IDX].split(',')]
            else:
                possible_keys[output[TABLE_IDX]] = []
        return possible_keys

    def create_table_if_absences(self, table_name, column_2_value, primary_key_column=None,
                                 enable_autoincrement_id_key=True):
        """
        Create a table according to parameters if absences. This function will not insert any data into the table.
        The column names and types of the table will be inferred from `column_2_value`.

        :param table_name: the name of the table you want to create
        :param column_2_value: a dict, whose keys are the names of columns and values. This data will be used to infer the column names and types of the table.
        :param primary_key_column: A column name in `column_2_value`. The corresponding column will be set as primary key. Otherwise, there will be no primary key.
        :param enable_autoincrement_id_key: If it is True, the `primary_key_column` will be autoincrement. It is only meaningful when `primary_key_column` is not None.
        """
        self._connect_if_loss()
        if primary_key_column is not None and primary_key_column not in column_2_value:
            raise RuntimeError("the primary key column {} is not in column_2_value".format(primary_key_column))

        if not self.exist_table(table_name):
            column_2_type = self._to_db_data_type(column_2_value)
            columns = []
            for column, column_type in column_2_type.items():
                if column_type == String:
                    column_type = Text(65535)
                if column == primary_key_column:
                    columns.append(
                        Column(column, column_type, primary_key=True, autoincrement=enable_autoincrement_id_key))
                else:
                    columns.append(Column(column, column_type))
            table = Table(table_name, self.metadata, *columns, extend_existing=True)
            table.create(self.engine)

    def get_explain_sql(self, sql, format = 'json'):
        """
        Constructs an EXPLAIN SQL statement for a given SQL query.

        :param sql: The SQL query to explain.
        :param execute: A boolean flag indicating whether to execute the query plan.
        :param comment:  A SQL comment will be added to the beginning of the SQL query.
        :return: The result of executing the `EXPLAIN` SQL statement.
        """
        result = str()
        if format == 'json':
            result = "EXPLAIN format=json {}".format(sql)
        else:
            result = "EXPLAIN {}".format(sql)
        return result

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
            conn.execute(text("SET SESSION MAX_EXECUTION_TIME={};".format(self.config.sql_execution_timeout * 1000)))
            result = conn.execute(text(sql) if isinstance(sql, str) else sql)
            if fetch:
                row = result.all()
                if fetch_column_name:
                    row = [tuple(result.keys()), *row]
        except OperationalError as e:
            if "timed out" in str(e) or "time exceeded" in str(e):
                raise DBStatementTimeoutException(str(e))
            else:
                raise e
        # except Exception as e:
        #     if "PilotScopePullEnd" not in str(e):
        #         raise e
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