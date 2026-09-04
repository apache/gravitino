CREATE SCHEMA gt_mysql.gt_db_projection;

CREATE TABLE gt_mysql.gt_db_projection.tb01 (
    id int,
    name varchar
);

insert into gt_mysql.gt_db_projection.tb01(id, name) values (1, 'sam'), (2, 'jerry');

-- Selecting every column together with a computed one pushes the projection into the internal
-- connector, which types the unbounded varchar column as varchar(65535). Since Trino 444 the plan
-- is rejected if that type reaches the engine instead of the one this connector declared.
select *, if(name is null, '', name) from gt_mysql.gt_db_projection.tb01 order by id;

drop table gt_mysql.gt_db_projection.tb01;

drop schema gt_mysql.gt_db_projection;
