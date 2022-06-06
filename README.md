# CS223Project

##数据库配置
先创建3个database

数据库名：leader, follower1, follower2

在每一个db中执行：
```
create table data_item
(
    key   varchar(100)
        constraint data_item_pk
            primary key,
    value int not null
);

create unique index data_item_key_uindex
    on data_item (key);
```

运行：Launch.class

Create a `logs/` folder under the project root directory if not already there.

每次跑完记得删除`logs/`底下所有文件，并删除follower3这个数据库
