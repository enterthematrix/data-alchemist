use data_alchemist;
create schema if not exists cortex;

-- query to get the list of all the packages available in Snowflake
select * from SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES;
select * from SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES where package_name = 'snowflake-snowpark-python';
select distinct runtime_version from SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES order by runtime_version desc;

-- Simple procedure to return a number as string with a '+' sign prepended to it
create or replace procedure proc1(num float)
  returns string
  language python
  runtime_version = '3.9'
  packages = ('snowflake-snowpark-python==1.13.0')
  handler = 'proc1'
as $$
import snowflake.snowpark as snowpark
def proc1(session: snowpark.Session, num: float):
  query = f"select '+' || to_char({num})"
  return session.sql(query).collect()[0][0]
$$;

call proc1(22.5);

-- Simple UDF to return a number as string with a '+' sign prepended to it
create or replace function proc2(num float)
  returns string
  language python
  runtime_version = '3.9'
  -- packages = ('snowflake-snowpark-python')
  handler = 'proc2'
as $$
# import snowflake.snowpark as snowpark
def proc2(num: float):
  return '+' + str(num)
$$;

select proc2(22.5);

-- Simple UDTF to return a number as string with a '+' sign prepended to it

create or replace function proc3(s string)
  returns table(out varchar)
  language python
  runtime_version = '3.9'
  -- packages = ('snowflake-snowpark-python')
  handler = 'MyClassS'
as $$
# import snowflake.snowpark as snowpark
class MyClassS:
  def process(self, s: str):
    yield (s,)
    yield (s,)
$$;

select * from table(proc3('abc'));




-- Regular Python UDF
create or replace function add_2(x float, y float)
    returns float
    language python
    runtime_version = 3.9
    handler = 'add_2'
as $$
def add_2(x, y):
  return x + y
$$;

-- Vectorized Python UDF
create or replace function add_v(x float, y float)
    returns float
    language python
    runtime_version = 3.9
    packages = ('pandas')
    handler = 'add_v'
as $$
import pandas
from _snowflake import vectorized

@vectorized(input=pandas.DataFrame, max_batch_size=100)
def add_v(df):
  return df[0] + df[1]
$$;

create or replace table xy(x float, y float);
-- insert into xy values (1.0, 3.14), (2.2, 1.59), (3.0, -0.5);
INSERT INTO xy
SELECT 
  UNIFORM(0, 1000, RANDOM())::FLOAT AS x,
  UNIFORM(0, 1000, RANDOM())::FLOAT AS y
FROM TABLE(GENERATOR(ROWCOUNT => 100000));


select x, y, add_2(x, y) from xy;

-- Below query even though it is using the vectorized function, will take longer time to run due to the overhead of spinning UDF runtime and load pandas
-- Vectorization pays off for more complex logic
select x, y, add_v(x, y) from xy;



