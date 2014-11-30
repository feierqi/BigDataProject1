A = load 'transactions.txt' USING PigStorage(',') as (TransID, CustID, TransTotal, TransNumItems, TransDesc);
B = Group A by CustID
C = foreach B Generate group as CustID, COUNT(A), SUM(A.TransTotal);
STORE C INTO 'pig2';