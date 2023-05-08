create external table Account (
    AccountID int,
    AccountNum varchar(255),
    ClientId int,
    DateOpen date
) as copy from '/tmp/input/Account.csv' delimiter ';';

create external table Clients(
    ClientId int,
    ClientName varchar(255),
    Type char(2),
    Form char(6),
    RegisterDate date
) as copy from '/tmp/input/Clients.csv' delimiter ';';

create external table Rate_raw (
    Currency char(3),
    Rate varchar(10),
    RateDate date
) as copy from '/tmp/input/Rate.csv' delimiter ';';

create external table Operation (
    AccountDB int,
    AccountCR int,
    DateOp date,
    Amount float,
    Currency char(3),
    Comment varchar

) as copy from '/tmp/input/Operation.csv' delimiter ';';

create table Rate as select Currency, cast(replace(Rate, ',', '.') as float) as Rate, RateDate from Rate_raw;

--select * from projections;
--select * from storage_containers;
