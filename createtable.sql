create table brand (
  code varchar(16) not null,
  name varchar(128) not null,
  market varchar(32) not null,
  category varchar(16) not null,
  PRIMARY KEY (code,market)
);
create index idx_brand_code on brand (code);

create table stock (
  day date not null,
  code varchar(16) not null,
  name varchar(128) not null,
  market varchar(32) not null,
  category varchar(16) not null,
  openingPrice float8 null,
  highPrice float8 null,
  lowPrice float8 null,
  closingPrice float8 null,
  volumeOfTrading float8 null,
  tradingValue float8 null,
  PRIMARY KEY (day, code, market)
);

create index idx_stock_day on stocks (day);
create index idx_stock_code on stocks (code);
create index idx_stock_name on stocks (name);
create index idx_stock_market on stocks (market);
create index idx_stock_category on stocks (category);

insert into brand(code,name,market,category)
select s.code,s.name,s.market,s.category
from stock s,
     (select code, market, max(day) as day
      from stock
      group by code, market) t
where
        s.code = t.code
    and s.market = t.market
    and s.day = t.day;
