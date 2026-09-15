-- This is auto-generated code
/*
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://esooliststorageaccount.dfs.core.windows.net/esoolistdata/silver',
        FORMAT = 'PARQUET'
    ) AS [result]
*/

-- https://esooliststorageaccount.blob.core.windows.net/esoolistdata/silver/part-00000-7d5de27e-00e2-4989-921a-77bac802bb6e.c000.snappy.parquet


-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://esooliststorageaccount.blob.core.windows.net/esoolistdata/silver',
        FORMAT = 'PARQUET'
    ) AS [result]


    