A = load 'customers.txt' USING PigStorage(',') as (ID, Name, Age, CountryCode, Salary);
B = load 'transactions.txt' USING PigStorage(',') as (TransID, CustID, TransTotal, TransNumItems, TransDesc);
C = foreach A generate ID as CustID, CountryCode
D = foreach B Generate CustID, TransTotal
E = join C by CustID, D by CustID
F = gouup E by CountryCode
G = group E by CustID
H = foreach G generate group as CustID, CountryCode, SUM(E.TransTotal) as TransTotal
I = foreach F Generate group as CountryCode, COUNT(E.CustID), MAX(H.TransTotal), MIN(H.TransTotal);
STORE I INTO 'pig4';