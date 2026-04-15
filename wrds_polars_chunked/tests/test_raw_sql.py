from unittest import mock


def test_rawsql_takes_unparameterized_sql(mock_connection):
    """Test raw_sql handles unparameterized SQL queries."""
    with mock.patch("wrds_polars_chunked.sql.sa"):
        with mock.patch("wrds_polars_chunked.sql.pl") as mock_pl:
            mock_connection.connection = mock.Mock()
            mock_connection.engine = mock.Mock()
            sql = "SELECT * FROM information_schema.tables LIMIT 1"
            mock_connection.raw_sql(sql, "idx")
            mock_pl.read_database_uri.assert_called_once_with(
                sql,
                mock_connection.connection.engine.url.render_as_string(),
                partition_on="idx",
                partition_range=500000,
                schema_overrides=None,
            )
