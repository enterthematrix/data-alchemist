{% if execute %}
    {{ calculate_aqi_udf() }}
    {{ aqi_category_udf() }}
    {{ prominent_pollutant_udf() }}
{% endif %}
