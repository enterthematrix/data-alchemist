import random
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
from faker import Faker
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

session = Session.builder.configs(SnowflakeLoginOptions("sanju")).create()
session.use_database("DATA_ALCHEMIST")
session.use_schema("CORTEX")
session.use_warehouse("ADHOC_WH")
session.use_role("SYSADMIN")

def main(session: snowpark.Session):
    f = Faker()
    output = [[f.name(), f.country(), f.city(), f.state(), random.randrange(100, 10000)]
        for _ in range(10000)]

    schema = StructType([ 
        StructField("NAME", StringType(), False),  
        StructField("COUNTRY", StringType(), False), 
        StructField("CITY", StringType(), False),  
        StructField("STATE", StringType(), False),  
        StructField("SALES", IntegerType(), False)])
    df = session.create_dataframe(output, schema)
    df.write.mode("overwrite").save_as_table("CUSTOMERS_FAKE")
    df.show()
    return df

if __name__ == "__main__":
    main(session)