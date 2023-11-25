<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->


docker-compose up：启动容器
docker-compose down：停止容器
docker-compose build：构建容器
docker-compose ps：列出容器
docker-compose logs：查看容器日志
docker-compose exec：在容器中执行命令

https://github.com/datastrato/gravitino/issues/386
```shell
docker exec -it playground_metalake_1 bash
```

```shell

CREATE SCHEMA "playground_metalake.playground_hive".db1 WITH (location = 'hdfs://hive:9000/user/hive/warehouse/db1.db');

show create schema "playground_metalake.playground_hive".db1;

create table "playground_metalake.playground_hive".db1.table_001
(
name varchar,
salary varchar
)
WITH (
format = 'TEXTFILE'
);

insert into "playground_metalake.playground_hive".db1.table_001 (name, salary) values ('sam', '11');

select * from "playground_metalake.playground_hive".db1.table_001;
```
