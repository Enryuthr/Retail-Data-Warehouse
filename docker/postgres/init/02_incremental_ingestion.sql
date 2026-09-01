CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.ingestion_batches (
    batch_id text PRIMARY KEY,
    pipeline_name text NOT NULL,
    logical_date date NOT NULL,
    source_file text,
    file_checksum text,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    status text NOT NULL CHECK (status IN ('started', 'ingested', 'completed', 'failed')),
    rows_received bigint NOT NULL DEFAULT 0,
    rows_inserted bigint NOT NULL DEFAULT 0,
    rows_updated bigint NOT NULL DEFAULT 0,
    rows_deleted bigint NOT NULL DEFAULT 0,
    rows_rejected bigint NOT NULL DEFAULT 0,
    last_source_updated_at timestamptz,
    error_message text,
    UNIQUE (pipeline_name, logical_date)
);

CREATE TABLE IF NOT EXISTS control.ingested_files (
    pipeline_name text NOT NULL,
    logical_date date NOT NULL,
    source_file text NOT NULL,
    batch_id text NOT NULL REFERENCES control.ingestion_batches(batch_id),
    file_checksum text NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    status text NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
    rows_received bigint NOT NULL DEFAULT 0,
    rows_inserted bigint NOT NULL DEFAULT 0,
    rows_updated bigint NOT NULL DEFAULT 0,
    rows_deleted bigint NOT NULL DEFAULT 0,
    rows_rejected bigint NOT NULL DEFAULT 0,
    last_source_updated_at timestamptz,
    error_message text,
    PRIMARY KEY (pipeline_name, logical_date, source_file),
    UNIQUE (source_file, file_checksum)
);

CREATE TABLE IF NOT EXISTS control.pipeline_watermarks (
    pipeline_name text PRIMARY KEY,
    last_logical_date date,
    batch_id text REFERENCES control.ingestion_batches(batch_id),
    last_source_updated_at timestamptz,
    updated_at timestamptz NOT NULL
);

DO $$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'customers_raw', 'promotions_raw', 'orders_raw', 'order_items_raw'
    ] LOOP
        IF to_regclass(format('bronze.%s', table_name)) IS NOT NULL THEN
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS batch_id text', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS logical_date date', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS source_file text', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS file_checksum text', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS source_updated_at timestamptz', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS ingested_at timestamptz', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS operation text', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS business_date date', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS source_row_number integer', table_name);
            EXECUTE format('ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS source_record_hash text', table_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON bronze.%I (source_updated_at)', 'ix_' || table_name || '_source_updated_at', table_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON bronze.%I (batch_id)', 'ix_' || table_name || '_batch_id', table_name);
        END IF;
    END LOOP;
END $$;
