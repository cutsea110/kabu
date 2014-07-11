create table stocks (
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

create index idx_day on stocks (day);
create index idx_code on stocks (code);
create index idx_name on stocks (name);
create index idx_market on stocks (market);
create index idx_category on stocks (category);

