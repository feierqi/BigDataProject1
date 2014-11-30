A = load 'customers.txt' USING PigStorage(',') as (ID, Name, Age, CountryCode, Salary);
B = load 'transactions.txt' USING PigStorage(',') as (TransID, CustID, TransTotal, TransNumItems, TransDesc);
C = foreach A generate ID as CustID, Name, Salary
D = foreach B Generate CustID, TransTotal, TransNumItems
E = join C by CustID, D by CustID
F = gouup E by CustID
G = foreach F Generate group as CustID, Name, Salary, COUNT(E.CustID), SUM(E.TransTotal), MIN(E.TransNumItems);
STORE G INTO 'pig3';