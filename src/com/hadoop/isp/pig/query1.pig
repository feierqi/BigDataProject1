A = load 'customers.txt' USING PigStorage(',') as (ID, Name, Age, CountryCode, Salary);
B = FILTER A BY (CountryCode >= 2 and CountryCode <= 6);
C = foreach B generate Name;
STORE C INTO 'pig1';