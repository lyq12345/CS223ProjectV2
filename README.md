# CS223Project

## Configuring db
First creat 3 postgre databases: `leader`, `follower1`, `follower2`

Execute this statement for each database：
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

Create a `logs/` folder under the project root directory if not already there.

Then run：`Launch.class`
