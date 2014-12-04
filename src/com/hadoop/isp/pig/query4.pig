customers = LOAD '/tmp/Customers.txt' USING PigStorage(',') AS (CustID:int, Name:chararray, Age:int, CountryCode:int, Salary:float);
transactions = LOAD '/tmp/Transactions.txt' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
mapped_customers = FOREACH customers GENERATE CustID, CountryCode;
mapped_transactions = FOREACH transactions GENERATE CustID, TransTotal;
cust_trans_info = JOIN mapped_customers BY CustID, mapped_transactions BY CustID;
country_cust_trans_info = GROUP cust_trans_info BY mapped_customers::CountryCode;
result = FOREACH country_cust_trans_info GENERATE group, COUNT(cust_trans_info), MIN(cust_trans_info.TransTotal), MAX(cust_trans_info.TransTotal);
STORE result INTO 'pig4' USING PigStorage(',');
