customers = LOAD '/tmp/Customers.txt' USING PigStorage(',') AS (ID:int, Name:chararray, Age:int, CountryCode:int, Salary:float);
filtered_customers = FILTER customers BY (CountryCode >= 2 AND CountryCode <= 6);
result = FOREACH filtered_customers GENERATE Name, CountryCode;
STORE result INTO 'pig1' USING PigStorage(',');
