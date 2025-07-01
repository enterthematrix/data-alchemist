{% macro variables_demo() %}

    {% set jinja_var = "Sanju" %}
    {{ log("Hello " ~ jinja_var, info=True) }}

    {{ log("Hello dbt user " ~ var("user_name", "NO USERNAME IS SET!!") ~ "!", info=True) }}

    {% if var("test_var", False) %}
       {{ log("test_var:  " ~ var("in_test"), info=True) }}
    {% else %}
       {{ log("test_var is NOT SET !! ", info=True) }}
    {% endif %}

{% endmacro %}