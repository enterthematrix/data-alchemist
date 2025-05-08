-- ref: https://docs.snowflake.com/en/user-guide/ml-functions/forecasting
USE SCHEMA DATA_ALCHEMIST.CORTEX;

-- #################################################### DUMMY SALES DATA  ####################################################

-- Setting Up the Data

CREATE OR REPLACE TABLE sales_data (store_id NUMBER, item VARCHAR, date TIMESTAMP_NTZ,
  sales FLOAT, temperature NUMBER, humidity FLOAT, holiday VARCHAR);

INSERT INTO sales_data VALUES
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-01'), 2.0, 50, 0.3, 'new year'),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-02'), 3.0, 52, 0.3, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-03'), 4.0, 54, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-04'), 5.0, 54, 0.3, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-05'), 6.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-06'), 7.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-07'), 8.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-08'), 9.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-09'), 10.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-10'), 11.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-11'), 12.0, 55, 0.2, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-12'), 13.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-01'), 2.0, 50, 0.3, 'new year'),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-02'), 3.0, 52, 0.3, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-03'), 4.0, 54, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-04'), 5.0, 54, 0.3, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-05'), 6.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-06'), 7.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-07'), 8.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-08'), 9.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-09'), 10.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-10'), 11.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-11'), 12.0, 55, 0.2, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-12'), 13.0, 55, 0.2, NULL);

-- Future values for additional columns (features)
CREATE OR REPLACE TABLE future_features (store_id NUMBER, item VARCHAR,
  date TIMESTAMP_NTZ, temperature NUMBER, humidity FLOAT, holiday VARCHAR);

INSERT INTO future_features VALUES
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-13'), 52, 0.3, NULL),
  (1, 'jacket', TO_TIMESTAMP_NTZ('2020-01-14'), 53, 0.3, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-13'), 52, 0.3, NULL),
  (2, 'umbrella', TO_TIMESTAMP_NTZ('2020-01-14'), 53, 0.3, NULL);

-- Forecasting on a single series

CREATE OR REPLACE VIEW v1 AS SELECT date, sales
FROM sales_data WHERE store_id=1;
SELECT * FROM v1;

-- Traing the model for the single series
CREATE SNOWFLAKE.ML.FORECAST single_ts_model(
  INPUT_DATA => TABLE(v1),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
);

-- Forecasting on a single series
call single_ts_model!FORECAST(FORECASTING_PERIODS => 3);

-- Forecasting on multiple series

CREATE OR REPLACE VIEW v3 AS SELECT [store_id, item] AS store_item, date, sales FROM sales_data;
SELECT * FROM v3;

CREATE SNOWFLAKE.ML.FORECAST multiple_ts_model(
  INPUT_DATA => TABLE(v3),
  SERIES_COLNAME => 'store_item',
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
);

CALL multiple_ts_model!FORECAST(FORECASTING_PERIODS => 2);

-- Forecasting with additional features

CREATE OR REPLACE VIEW v2 AS SELECT date, sales, temperature, humidity, holiday
  FROM sales_data WHERE store_id=1 AND item='jacket';
SELECT * FROM v2;


CREATE SNOWFLAKE.ML.FORECAST ts_model_with_features(
  INPUT_DATA => TABLE(v2),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
);

-- To generate forecasts with this model, you must provide future values for the features to the model: in this case, TEMPERATURE, HUMIDITY and HOLIDAY. 
-- This allows the model to adjust its sales forecasts based on temperature, humidity, and holiday forecasts.

CREATE OR REPLACE VIEW v2_forecast AS select date, temperature, humidity, holiday
  FROM future_features WHERE store_id=1 AND item='jacket';
SELECT * FROM v2_forecast;


CREATE OR REPLACE TABLE sales_forecasts AS
SELECT * FROM TABLE(
  ts_model_with_features!FORECAST(
    INPUT_DATA => TABLE(v2_forecast),
    TIMESTAMP_COLNAME => 'date'
  )
);

select * from sales_forecasts;
call ts_model_with_features!SHOW_EVALUATION_METRICS();

-- The result is a table with the actual sales and the forecasted sales 
SELECT date AS ts, sales AS actual,
  NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
  FROM v2
  UNION ALL
SELECT ts, NULL AS actual,
  forecast, lower_bound, upper_bound
  FROM sales_forecasts;

 
-- #################################################### WEATHER FORECASTING  ####################################################
Data @ data/weather-ts.parquet

SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT';

-- data preparation
with cte as (
    SELECT date FROM WEATHER
    WHERE name = 'ALBANY INTERNATIONAL AIRPORT')
select date, count(*) as cnt
from cte
group by date
having cnt > 1;

SELECT * FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
AND (date = '2022-02-28' or date = '2018-02-28');

DELETE FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
    AND (date = '2022-02-28' AND temp = 29.7
    OR date = '2018-02-28' AND temp = 41.2);


-- create forecast
create snowflake.ml.forecast weather_forecast(
    input_data => SYSTEM$QUERY_REFERENCE($$
SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
$$),
    timestamp_colname => 'ts',
    target_colname => 'temp');

-- Forecasting
CREATE OR REPLACE TABLE weather_forecast AS
SELECT * FROM TABLE(
  weather_forecast!FORECAST(
    forecasting_periods => 30,
    config_object => {'prediction_interval': 0.9}
  )
);
-- The result is a table with the actual temperature and the forecasted temperature
SELECT date AS ts, temp as actual,
    NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM WEATHER
    WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
UNION ALL
SELECT ts, NULL AS actual,
    forecast, lower_bound, upper_bound
    FROM  weather_forecast;