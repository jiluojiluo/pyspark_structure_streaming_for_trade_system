create database sparkStreaming;
use sparkStreaming;


drop table if exists StockBook;
create table
stockBook(
securityId char(8) default ' '  not null,
investorId char(20) default ' ' not null,
stockBookUnit char(8) default ' ' not null,
remainingBalance decimal(17,2) default '0.00' not null,
remainingBalanceBefore decimal(17,2) default '0.00' not null,
transactionFlag char(1) default ' ' not null,
bookdate date,
primary key USING BTREE (securityId,investorId,stockBookUnit),
INDEX idx_securityId USING BTREE(securityId),
INDEX idx_investorId using BTREE(investorId),
INDEX idx_bookdate using BTREE(bookdate)
);

select * from sparkstreaming.stockBook;

drop table if exists tradeDetail;
create table
tradeDetail(
id bigint auto_increment,
securityId char(8) default ' '  not null,
investorId char(20) default ' ' not null,
tradeUnit char(8) default ' ' not null,
counterpartyInvestorId char(20) default ' ' not null,
counterpartyTradeUnit char(8) default ' ' not null,
tradePrice decimal(17,2) default '0.00' not null,
tradeVolume decimal(17,2) default '0.00' not null,
tradeType char(20) default ' ' not null,
tradeTime datetime,
tradedate date,
primary key USING BTREE (id),
INDEX idx_trade USING BTREE(securityId,investorId,tradeUnit),
INDEX idx_securityId USING BTREE(securityId),
INDEX idx_investorId using BTREE(investorId),
INDEX idx_bookdate using BTREE(tradedate)
);

select * from sparkstreaming.tradeDetail;
