-- Create Schema
CREATE SCHEMA gold

create view gold.final
AS
SELECT *
FROM
    OPENROWSET(
        BULK 'https://esooliststorageaccount.dfs.core.windows.net/esoolistdata/silver',
        FORMAT = 'PARQUET'
    ) AS result1


select * from gold.final