import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

#@st.cache_data(ttl=3600, show_spinner="Connecting with Snowflake...")
def get_engine():
    """Fetches AQI fact data with location and timestamp info."""
    # Replace with your Snowflake credentials
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],  
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"],
            warehouse=st.secrets["snowflake"]["warehouse"]
        )
    )
    return engine


#@st.cache_data(ttl=3600, show_spinner="Running query...")
def run_query(_query: str) -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        raise RuntimeError("Failed to create SQLAlchemy engine.")
    return pd.read_sql(_query, engine)