from unittest import mock


def test_rawsql_takes_unparameterized_sql(mock_connection):
    """Test raw_sql handles unparameterized SQL queries."""
    with mock.patch("wrds_polars_chunked.sql.sa"):
        with mock.patch("wrds_polars_chunked.sql.pl") as mock_pl:
            mock_connection.connection = mock.Mock()
            mock_connection.engine = mock.Mock()
            sql = "SELECT * FROM information_schema.tables LIMIT 1"
            mock_connection.raw_sql(sql)
            mock_pl.read_database.assert_called_once_with(
                sql,
                mock_connection.connection,
                iter_batches=True,
                batch_size=500000,
                execute_options={"parameters": None},
                SchemaOverrides=None,
            )


def test_rawsql_takes_parameterized_sql(mock_connection):
    """Test raw_sql handles parameterized SQL queries."""
    with mock.patch("wrds_polars_chunked.sql.sa"):
        with mock.patch("wrds_polars_chunked.sql.pl") as mock_pl:
            mock_connection.connection = mock.Mock()
            mock_connection.engine = mock.Mock()
            sql = (
                "SELECT * FROM information_schema.tables "
                "WHERE table_name = %(tablename)s LIMIT 1"
            )
            tablename = "pg_stat_activity"
            mock_connection.raw_sql(sql, params=tablename)
            mock_pl.read_database.assert_called_once_with(
                sql,
                mock_connection.connection,
                iter_batches=True,
                batch_size=500000,
                execute_options={"parameters": "pg_stat_activity"},
                SchemaOverrides=None,
            )
