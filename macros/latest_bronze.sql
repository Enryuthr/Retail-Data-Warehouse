{% macro latest_bronze(table_name, key_column) -%}
(
    select *
    from (
        select
            raw_source.*,
            row_number() over (
                partition by raw_source.{{ key_column }}
                order by raw_source.source_updated_at desc nulls last,
                         raw_source.ingested_at desc nulls last,
                         raw_source.batch_id desc nulls last,
                         raw_source.source_record_hash desc nulls last
            ) as _version_rank
        from {{ source('bronze', table_name) }} as raw_source
    ) versioned
    where _version_rank = 1
      and coalesce(operation, 'I') <> 'D'
)
{%- endmacro %}
