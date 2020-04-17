

# 查询数据库占用磁盘空间
select TABLE_SCHEMA,
       concat(truncate(sum(data_length)/1024/1024,2),' MB') as data_size,
       concat(truncate(sum(index_length)/1024/1024,2),' MB') as index_size
from information_schema.tables
group by TABLE_SCHEMA;

# 创建表
create table stock.stock_attr
   (
       id        int auto_increment        primary key,
       item      varchar(35)                                  null,
       value     varchar(500)                                  null
   )
       charset = utf8;

use stock;

truncate table stock_basic;

drop table stock_basic;

alter table stock_basic modify ts_code varchar(10) not null comment 'TS代码';

create unique index stock_basic_ts_code_uindex
       	on stock_basic (ts_code);

alter table stock_basic
       	add id int auto_increment first;