\getenv reader_password GRAFANA_AIRFLOW_DB_PASSWORD
DO $role$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_airflow_reader') THEN
        CREATE ROLE grafana_airflow_reader LOGIN CONNECTION LIMIT 5;
    END IF;
END
$role$;
ALTER ROLE grafana_airflow_reader LOGIN CONNECTION LIMIT 5 PASSWORD :'reader_password';
ALTER ROLE grafana_airflow_reader SET default_transaction_read_only = on;
ALTER ROLE grafana_airflow_reader SET statement_timeout = '10s';
GRANT CONNECT ON DATABASE airflow TO grafana_airflow_reader;
GRANT USAGE ON SCHEMA public TO grafana_airflow_reader;
GRANT SELECT ON public.dag, public.dag_run, public.task_instance, public.job
    TO grafana_airflow_reader;
