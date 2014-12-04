transactions = LOAD '/tmp/Transactions.txt' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
grouped_transactions = GROUP transactions by CustID;
result = FOREACH grouped_transactions GENERATE group, COUNT(transactions), SUM(transactions.TransTotal);
STORE result INTO 'pig2_2nd_try' USING PigStorage(',');
