<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# Playground
This is a complete Gravitino runtime environment with `Hive`, `Hdfs`, `Trino`, and `Gravitno` Server. just execute the `./luanch-playgraound.sh` script.
It will automatically start the `gravitino-ci-hive`, `gravitino-ci-trino`, and `Gravitino` Docker containers on the local host. 
Depending on your network, the startup may take 3-5 minutes.
Once the playground environment has started, you can open http://localhost:8090 to access the Gravitino Web UI.
And use a Trino client (such as Datagrip) to test Gravitino by connecting to the Trino Docker continer via `jdbc:trino://127.0.0.1:8080`.
You test in Trino using the following SQL

```shell
CREATE SCHEMA "playground_metalake.playground_hive".db1 
WITH (location = 'hdfs://hive:9000/user/hive/warehouse/db1.db');

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