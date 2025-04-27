-- ============================================================ Diamonds dataset ============================================================
create schema if not exists DATA_ALCHEMIST.DIAMONDS;
USE SCHEMA DATA_ALCHEMIST.DIAMONDS;

CREATE OR REPLACE FILE FORMAT CSV_HEADER_LIST
    TYPE='CSV' SKIP_HEADER=1;

CREATE OR REPLACE STAGE INT_STAGE
    FILE_FORMAT=CSV_HEADER_LIST;

-- to list all: https://sfquickstarts.s3.us-west-1.amazonaws.com/
-- to download: https://sfquickstarts.s3.us-west-1.amazonaws.com/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv
CREATE OR REPLACE STAGE EXT_STAGE_LIST
    FILE_FORMAT=CSV_HEADER_LIST
    URL='s3://sfquickstarts/intro-to-machine-learning-with-snowpark-ml-for-python/';

LIST @EXT_STAGE_LIST;

SELECT METADATA$FILE_ROW_NUMBER as RowId, $1, $2, $3
FROM @EXT_STAGE_LIST/diamonds.csv
LIMIT 10;


-- ============================================================ Titanic dataset ============================================================

USE SCHEMA DATA_ALCHEMIST.CORTEX;

-- relative path is not working for some reason
-- PUT file://../../data/titanic.csv @DATA_ALCHEMIST.CORTEX.INT_STAGE
-- OVERWRITE=true AUTO_COMPRESS=false;
PUT file:///Users/sanju/workspace/data-alchemist/data/titanic.csv @DATA_ALCHEMIST.CORTEX.INT_STAGE
OVERWRITE=true AUTO_COMPRESS=false;

LIST @INT_STAGE;

SELECT METADATA$FILE_ROW_NUMBER as RowId, $1, $2, $3
FROM @INT_STAGE/titanic.csv
LIMIT 10;

CREATE OR REPLACE TABLE TITANIC (
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
	"Embarked" VARCHAR);

COPY INTO TITANIC
FROM @INT_STAGE/titanic.csv
	FILE_FORMAT = (
		TYPE='CSV'
		SKIP_HEADER=1
		FIELD_OPTIONALLY_ENCLOSED_BY='"');

SELECT *
FROM TITANIC
LIMIT 10;

-- ============================================================ IMDB dataset ============================================================

CREATE OR REPLACE DATABASE IMDB;
USE SCHEMA IMDB.PUBLIC;

CREATE FILE FORMAT CSV_COMMA_DELIMITER
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('\\N');

CREATE STAGE FILES;
CREATE STAGE MODELS;

PUT file:///Users/sanju/workspace/data-alchemist/data/imdb_train.csv.gz @FILES OVERWRITE=true AUTO_COMPRESS=false;
PUT file:///Users/sanju/workspace/data-alchemist/data/imdb_test.csv.gz @FILES OVERWRITE=true AUTO_COMPRESS=false;

CREATE TABLE TRAIN_DATASET (REVIEW STRING, SENTIMENT STRING);
CREATE TABLE TEST_DATASET (REVIEW STRING, SENTIMENT STRING);

COPY INTO TRAIN_DATASET FROM @FILES/imdb_train.csv.gz
	FILE_FORMAT = (FORMAT_NAME=CSV_COMMA_DELIMITER);
COPY INTO TEST_DATASET FROM @FILES/imdb_test.csv.gz
	FILE_FORMAT = (FORMAT_NAME=CSV_COMMA_DELIMITER);

REMOVE @FILES;

SELECT * FROM TRAIN_DATASET LIMIT 3;
SELECT * FROM TRAIN_DATASET WHERE SENTIMENT IS NULL;

SELECT * from TEST_DATASET LIMIT 3;
