{% macro text_clean(column_name) -%}
    nullif(trim({{ column_name }}::text), '')
{%- endmacro %}

{% macro normalize_token(expression) -%}
    regexp_replace(lower({{ expression }}), '[^a-z0-9]+', '_', 'g')
{%- endmacro %}

{% macro parse_date(expression) -%}
    case
        when {{ expression }} ~ '^\d{4}-\d{2}-\d{2}'
         and substring({{ expression }} from 6 for 2)::integer between 1 and 12
         and substring({{ expression }} from 9 for 2)::integer between 1 and extract(
                day from (
                    date_trunc(
                        'month',
                        make_date(
                            substring({{ expression }} from 1 for 4)::integer,
                            substring({{ expression }} from 6 for 2)::integer,
                            1
                        )
                    ) + interval '1 month - 1 day'
                )
            )::integer
            then make_date(
                substring({{ expression }} from 1 for 4)::integer,
                substring({{ expression }} from 6 for 2)::integer,
                substring({{ expression }} from 9 for 2)::integer
            )::timestamp
    end
{%- endmacro %}

{% macro bool_value(expression) -%}
    case
        when lower({{ expression }}) in ('true', 't', 'yes', 'y', '1') then true
        when lower({{ expression }}) in ('false', 'f', 'no', 'n', '0') then false
    end
{%- endmacro %}
