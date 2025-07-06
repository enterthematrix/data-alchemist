{% macro aqi_category_udf() %}
    CREATE OR REPLACE FUNCTION {{ target.database }}.{{ target.schema }}.get_aqi_category(aqi FLOAT)
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    HANDLER = 'get_category'
    AS
    $$
def aqi_category(aqi):
    if aqi is None:
        return "Unknown"
    if 0 <= aqi <= 50:
        return "Good"
    elif 51 <= aqi <= 100:
        return "Satisfactory"
    elif 101 <= aqi <= 200:
        return "Moderate"
    elif 201 <= aqi <= 300:
        return "Poor"
    elif 301 <= aqi <= 400:
        return "Very Poor"
    elif 401 <= aqi <= 500:
        return "Severe"
    else:
        return "Out of Range"
    $$;
{% endmacro %}
