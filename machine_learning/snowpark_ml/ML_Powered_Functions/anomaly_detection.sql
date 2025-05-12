USE SCHEMA DATA_ALCHEMIST.CORTEX;

SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT';


-- Train the model using weather data before '2021-01-01' | No labels
create or replace snowflake.ml.ANOMALY_DETECTION WEATHER_AD(
    input_data => SYSTEM$QUERY_REFERENCE($$
SELECT date::TIMESTAMP_NTZ as ts, temp
FROM WEATHER
WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
AND date < '2021-01-01'
$$),
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME => 'temp',
    LABEL_COLNAME => '');
SHOW SNOWFLAKE.ML.ANOMALY_DETECTION;
    

-- Test the model using weather data after '2021-01-01'

CREATE OR REPLACE TABLE weather_anomaly_results AS
SELECT * FROM TABLE(
  WEATHER_AD!detect_anomalies(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE($$
      SELECT date::TIMESTAMP_NTZ as ts, temp
      FROM WEATHER
      WHERE name = 'ALBANY INTERNATIONAL AIRPORT'
        AND date >= '2021-01-01'
    $$),
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME => 'temp'
  )
);

-- Show the results
select * from weather_anomaly_results where is_anomaly = 'true';
