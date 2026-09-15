
-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Educatormaster80#';
-- CREATE DATABASE SCOPED CREDENTIAL esodevops WITH IDENTITY = 'Managed Identity';
-- select * from sys.databaase_credentials;

CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://esooliststorageaccount.dfs.core.windows.net/esoolistdata/gold',
    CREDENTIAL = esodevops
);

CREATE EXTERNAL TABLE gold.finaltable WITH (
        LOCATION = 'Serving',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final2;