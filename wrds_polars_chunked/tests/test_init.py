from unittest import mock

import wrds_polars_chunked


def test_init_calls_sqlalchemy_create_engine_defaults():
    """Test init calls sqlalchemy create_engine with default parameters."""
    with mock.patch("wrds_polars_chunked.sql.sa") as mock_sa:
        wrds_polars_chunked.Connection()
        # The connection string now includes :@ for empty username and password
        connstring = "postgresql://:@{host}:{port}/{dbname}"
        connstring = connstring.format(
            host=wrds_polars_chunked.sql.WRDS_POSTGRES_HOST,
            port=wrds_polars_chunked.sql.WRDS_POSTGRES_PORT,
            dbname=wrds_polars_chunked.sql.WRDS_POSTGRES_DB,
        )
        mock_sa.create_engine.assert_called_with(
            connstring,
            execution_options={'stream_results': True},
            connect_args={"sslmode": "require", "application_name": wrds_polars_chunked.sql.appname},
        )


def test_init_calls_sqlalchemy_create_engine_custom():
    """Test init calls sqlalchemy create_engine with custom username."""
    with mock.patch("wrds_polars_chunked.sql.sa") as mock_sa:
        username = "faketestusername"
        # The connection string now includes : for empty password
        connstring = "postgresql://{usr}:@{host}:{port}/{dbname}"
        connstring = connstring.format(
            usr=username,
            host=wrds_polars_chunked.sql.WRDS_POSTGRES_HOST,
            port=wrds_polars_chunked.sql.WRDS_POSTGRES_PORT,
            dbname=wrds_polars_chunked.sql.WRDS_POSTGRES_DB,
        )
        wrds_polars_chunked.Connection(wrds_username=username)
        mock_sa.create_engine.assert_called_with(
            connstring,
            execution_options={'stream_results': True},
            connect_args={"sslmode": "require", "application_name": wrds_polars_chunked.sql.appname},
        )


def test_init_default_connect():
    """Test init connects by default."""
    with mock.patch("wrds_polars_chunked.sql.Connection.load_library_list"):
        with mock.patch("wrds_polars_chunked.sql.Connection.connect") as mock_connect:
            wrds_polars_chunked.Connection()
            mock_connect.assert_called_once()


def test_init_autoconnect_false_no_connect():
    """Test init does not connect when autoconnect=False."""
    with mock.patch("wrds_polars_chunked.sql.Connection.connect") as mock_connect:
        wrds_polars_chunked.Connection(autoconnect=False)
        mock_connect.assert_not_called()


def test_init_default_load_library_list():
    """Test init loads library list by default."""
    with mock.patch("wrds_polars_chunked.sql.Connection.connect"):
        with mock.patch("wrds_polars_chunked.sql.Connection.load_library_list") as mock_lll:
            wrds_polars_chunked.Connection()
            mock_lll.assert_called_once()


def test_init_autoconnect_false_no_load_library_list():
    """Test init does not load library list when autoconnect=False."""
    with mock.patch("wrds_polars_chunked.sql.Connection.connect"):
        with mock.patch("wrds_polars_chunked.sql.Connection.load_library_list") as mock_lll:
            wrds_polars_chunked.Connection(autoconnect=False)
            mock_lll.assert_not_called()
