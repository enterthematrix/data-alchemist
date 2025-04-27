import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

class SnowflakeSessionManager:
    _session = None

    @staticmethod
    def get_session():
        """Returns a Snowflake session, creating it if it doesn't exist."""
        if SnowflakeSessionManager._session is None:
            SnowflakeSessionManager._session = SnowflakeSessionManager.create_session()
        return SnowflakeSessionManager._session

    @staticmethod
    def create_session():
        """Create and return a new Snowflake session."""
        return Session.builder.configs(SnowflakeLoginOptions("sanju")).create()

