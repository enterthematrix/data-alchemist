import streamlit as st
import re
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def get_snowflake_connection(account, username, password):
    try:
        engine = create_engine(
            f'snowflake://{username}:{password}@{account}/AIRBNB/DEV?warehouse=COMPUTE_WH&role=ACCOUNTADMIN&account_identifier={account}'
        )
        connection = engine.connect()
        st.success("✅ Successfully connected to Snowflake.")
        return connection
    except SQLAlchemyError as e:
        st.error(f"❌ Failed to connect to Snowflake:\n{str(e)}")
        return None

def execute_sqls(connection, sql_blocks):
    for sql in sql_blocks:
        try:
            st.code(sql, language='sql')
            result = connection.execute(sa.text(sql))
            st.success("✅ Query executed successfully.")
        except SQLAlchemyError as e:
            st.error(f"❌ Error executing SQL:\n{sql}\n\n{str(e)}")

def main():
    st.title("📦 Snowflake SQL Runner")
    st.sidebar.info("""
    This app will do the following:

    🔹 Create the dbt user using the password dbt123 (change it)  
    🔹 Create the required warehouse / database / schema /role etc
    🔹 Import the raw AirBnB tables  
    """)

    pw = os.environ.get("SNOWFLAKE_PASSWORD") or ""
    hostname = st.text_input('Hostname', 'your-snowflake-account.identifier')
    username = st.text_input('Username', 'your_username')
    password = st.text_input('Password', pw, type="password")

    if st.button("Execute"):
        st.write(f"🔄 Attempting connection with: `{hostname}` as `{username}`")

        connection = get_snowflake_connection(hostname, username, password)
        if connection is None:
            return  # Stop if connection failed

        try:
            with open(os.path.join(CURRENT_DIR, "setup.md"), 'r') as file:
                md = file.read().rstrip()
        except Exception as e:
            st.error(f"❌ Failed to read markdown file: {str(e)}")
            return

        import_pattern = r'sql {#snowflake_setup}(.*?)```'
        match = re.search(import_pattern, md, re.DOTALL)

        if not match:
            st.warning("⚠️ No SQL block found with `{#snowflake_setup}` in `setup.md`.")
            return

        import_sqls = [row.strip() for row in match.group(1).split(';') if row.strip()]
        if not import_sqls:
            st.warning("⚠️ SQL block found, but no valid SQL statements to execute.")
            return

        st.write("📄 Parsed SQL statements:")
        execute_sqls(connection, import_sqls)

if __name__ == '__main__':
    main()