-- Create or replace a task to load Bitcoin data every 2 hours
CREATE OR REPLACE TASK BTC.BTC_SCHEMA.BTC_LOAD_TASK
  WAREHOUSE = LARGE_WH
  SCHEDULE = '6 HOUR'
AS
COPY INTO BTC.BTC_SCHEMA.BTC
FROM (
  SELECT
    t.$1:hash AS hashkey,
    t.$1:block_hash,
    t.$1:block_number,
    t.$1:block_timestamp,
    t.$1:fee,
    t.$1:input_value,
    t.$1:output_value AS output_btc,
    ROUND(t.$1:fee / t.$1:size, 12) AS fee_per_byte,
    t.$1:is_coinbase,
    t.$1:outputs
  FROM @BTC.BTC_SCHEMA.BTC_STAGE/transactions t
)
PATTERN = '.*/[0-9]{6,7}[.]snappy[.]parquet';


-- ⚠ IMPORTANT: Suspend the task if you are not continuing right away
-- This avoids unnecessary warehouse usage and costs

ALTER TASK BTC.BTC_SCHEMA.BTC_LOAD_TASK SUSPEND;
