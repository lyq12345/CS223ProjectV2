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

## Run
1. The project is built on `Maven`. Be sure to install the dependencies by clicking reload button in IDEA or run with: `mvn install` before running the project.
2. Modify the `username` and `password` in `jdbc.conf` with your postgre settings.

3. Create a `logs/` folder under the project root directory if not already there. After running the program, the logs will be generated here.

4. Then run：`Launch.class`
