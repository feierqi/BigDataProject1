customers = LOAD '/tmp/Customers.txt' USING PigStorage(',') AS (CustID:int, Name:chararray, Age:int, CountryCode:int, Salary:float);
transactions = load '/tmp/Transactions.txt' USING PigStorage(',') as (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
mapped_customers = FOREACH customers GENERATE CustID, Name, Salary;
mapped_transactions = FOREACH transactions GENERATE CustID, TransTotal, TransNumItems;
cust_trans_info = JOIN mapped_customers BY CustID, mapped_transactions BY CustID;
grouped_cust_trans_info = GROUP cust_trans_info BY (mapped_customers::CustID, mapped_customers::Name, mapped_customers::Salary);
result = FOREACH grouped_cust_trans_info GENERATE group, COUNT(cust_trans_info.TransTotal), SUM(cust_trans_info.TransTotal), MIN(cust_trans_info.TransNumItems);
STORE result INTO 'pig3_2nd_try' USING PigStorage(',');
