from session_manager import SnowflakeSessionManager
session = SnowflakeSessionManager.get_session()

query = "USE WAREHOUSE ADHOC_WH"
session.sql(query).collect()

query = "CREATE OR REPLACE STAGE DATA_ALCHEMIST.CORTEX.INT_STAGE"
session.sql(query).collect()

session.file.put(
    "../../data/titanic.csv",
    "DATA_ALCHEMIST.CORTEX.INT_STAGE",
    auto_compress=False,
    overwrite=True)

query = """
CREATE OR REPLACE TABLE DATA_ALCHEMIST.CORTEX.TITANIC (
	"PassengerId" INT,
	"Survived" INT,
	"Pclass" INT,
	"Name" VARCHAR,
	"Sex" VARCHAR,
	"Age" INT,
	"SibSp" INT,
	"Parch" INT,
	"Ticket" VARCHAR,
	"Fare" FLOAT,
	"Cabin" VARCHAR,
	"Embarked" VARCHAR)
"""
session.sql(query).collect()

query = """
COPY INTO DATA_ALCHEMIST.CORTEX.TITANIC
FROM @DATA_ALCHEMIST.CORTEX.INT_STAGE/titanic.csv
FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
"""
session.sql(query).collect()

query = """
SELECT *
FROM DATA_ALCHEMIST.CORTEX.TITANIC
LIMIT 10
"""
df = session.sql(query).to_pandas()
print(df.head())