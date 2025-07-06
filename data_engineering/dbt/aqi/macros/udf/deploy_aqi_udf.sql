{% macro deploy_all_udfs() %}
    {{ log("Creating AQI UDFs...", info=True) }}

    {{ create_calculate_aqi_udf() }}
    {{ create_aqi_category_udf() }}
    {{ create_prominent_pollutant_udf() }}

    -- Test the UDFs
    -- dbt run-operation deploy_all_udfs -- This will create the UDFs in the target database and schema
    {{ log("UDFs created successfully!", info=True) }}
    {{ log("Testing UDFs with sample data...", info=True) }}
    {% set result = run_query("SELECT " ~ target.database ~ "." ~ target.schema ~ ".calculate_aqi_udf(35, 50, 20, 10, 0.5, 40, 10)") %}
    {% set aqi_value = result.columns[0].values()[0] %}
    {{ log("Sample AQI value: " ~ aqi_value, info=True) }}
    {{ log("All UDFs created successfully!", info=True) }}
{% endmacro %}
