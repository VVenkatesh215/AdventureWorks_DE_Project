--select * from gold.customers;

create database scoped credential cred_venky
with 
    IDENTITY='Managed Identity'



create EXTERNAL data source source_silver
with 
(
    LOCATION='https://adlsazureproject.blob.core.windows.net/silver',
    credential=cred_venky
)



create EXTERNAL data source source_gold
with 
(
    LOCATION='https://adlsazureproject.blob.core.windows.net/gold',
    credential=cred_venky
)


CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE=PARQUET,
    DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec'
)


--------------------------------------
-- CREATE EXTERNAL TABLE gold.extsales
--------------------------------------
CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION='extsales',
    DATA_SOURCE=source_gold,
    FILE_FORMAT=format_parquet
)
AS 
select * from gold.sales


------------------------------------------
-- CREATE EXTERNAL TABLE gold.extcustomers
------------------------------------------
CREATE EXTERNAL TABLE gold.extcustomers
WITH
(
    LOCATION = 'extcustomers',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.customers;


-----------------------------------------
-- CREATE EXTERNAL TABLE gold.extproducts
-----------------------------------------
CREATE EXTERNAL TABLE gold.extproducts
WITH
(
    LOCATION = 'extproducts',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.products;


----------------------------------------
-- CREATE EXTERNAL TABLE gold.extreturns
----------------------------------------
CREATE EXTERNAL TABLE gold.extreturns
WITH
(
    LOCATION = 'extreturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.returns;


---------------------------------------
-- CREATE EXTERNAL TABLE gold.extsubcat
---------------------------------------
CREATE EXTERNAL TABLE gold.extsubcat
WITH
(
    LOCATION = 'extsubcat',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.subcat;


--------------------------------------------
-- CREATE EXTERNAL TABLE gold.extterritories
--------------------------------------------
CREATE EXTERNAL TABLE gold.extterritories
WITH
(
    LOCATION = 'extterritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.territories;

-----------------------------------------
-- CREATE EXTERNAL TABLE gold.extcalendar
-----------------------------------------
CREATE EXTERNAL TABLE gold.extcalendar
WITH
(
    LOCATION='extcalendar',
    DATA_SOURCE=source_gold,
    FILE_FORMAT=format_parquet
)
AS 
select * from gold.calendar








select * from gold.sales;







