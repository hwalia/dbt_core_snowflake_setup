-- Create the database
CREATE DATABASE BTC;

-- Create the schema within the BTC database
CREATE SCHEMA BTC_SCHEMA;

-- Create or replace the external stage pointing to the Bitcoin public S3 bucket
CREATE OR REPLACE STAGE BTC.BTC_SCHEMA.BTC_STAGE
  URL = 's3://aws-public-blockchain/v1.0/btc/'
  FILE_FORMAT = (TYPE = PARQUET);

-- List files available in the stage
LIST @BTC.BTC_SCHEMA.BTC_STAGE;

-- Create or replace a large warehouse for processing
    CREATE OR REPLACE WAREHOUSE LARGE_WH
  WAREHOUSE_SIZE = 'LARGE';

-- Query Bitcoin transaction data for a specific date - Please change to your own current date
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
FROM @BTC.BTC_SCHEMA.BTC_STAGE/transactions/date=2025-06-20 t;
