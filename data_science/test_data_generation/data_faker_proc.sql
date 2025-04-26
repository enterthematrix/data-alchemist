
/*
This is a Snowflake stored procedure that generates fake data using the Faker library.
It creates a table named 'fake_people' and populates it with fake names, addresses, cities, states, and emails.
The procedure takes two parameters: the number of rows to generate and the name of the table to insert the data into.
*/

-- To query the Faker package version available in Snowflake
select * from SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES where package_name like '%faker%';

create or replace procedure gen_fake_rows(r int,table_name string)
    returns string
    language python
    runtime_version = 3.12
    packages = ('faker==18.9.0', 'snowflake-snowpark-python==*')
    handler = 'main'
as
$$
import snowflake.snowpark as snowpark
from faker import Faker

def main(session: snowpark.Session, r: int, table_name: str):
    f = Faker()
    output = [
        [f.name(), f.address(), f.city(), f.state(), f.email()]
        for _ in range(r)
    ]
    df = session.create_dataframe(
        output,
        schema=["name", "address", "city", "state", "email"]
    )
    df.write.mode("append").save_as_table(table_name)
    return f"Inserted {r} rows into {table_name}"
$$;

call gen_fake_rows(1000,'fake_people');

select * from fake_people;




