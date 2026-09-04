\getenv reader_password GRAFANA_RETAIL_DB_PASSWORD
DO $role$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_retail_reader') THEN
        CREATE ROLE grafana_retail_reader LOGIN CONNECTION LIMIT 5;
    END IF;
END
$role$;
ALTER ROLE grafana_retail_reader LOGIN CONNECTION LIMIT 5 PASSWORD :'reader_password';
ALTER ROLE grafana_retail_reader SET default_transaction_read_only = on;
ALTER ROLE grafana_retail_reader SET statement_timeout = '10s';
GRANT CONNECT ON DATABASE retail_dw TO grafana_retail_reader;
GRANT USAGE ON SCHEMA control, silver TO grafana_retail_reader;
GRANT SELECT ON control.ingestion_batches, control.ingested_files,
    control.pipeline_watermarks, silver.data_quality_report TO grafana_retail_reader;
