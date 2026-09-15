-- Create new view
create view gold.final2
AS
SELECT *
FROM
    OPENROWSET(
        BULK 'https://esooliststorageaccount.dfs.core.windows.net/esoolistdata/silver',
        FORMAT = 'PARQUET'
    ) AS result2
WHERE order_status = 'delivered'


SELECT * FROM gold.final2