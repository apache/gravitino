CREATE SCHEMA gt_hive.gt_pbs_db1;

USE gt_hive.gt_pbs_db1;

CREATE TABLE nation (
  nationkey bigint,
  name varchar(25),
  regionkey bigint,
  comment varchar(152)
);

insert into nation select * from tpch.tiny.nation;

CREATE TABLE tb01 (
    n_nationkey bigint,
    n_name varchar,
    n_regionkey bigint,
    n_comment varchar,
    part_key varchar
) WITH (bucket_count = 4, bucketed_by = ARRAY['n_regionkey'], partitioned_by = ARRAY['part_key']);

INSERT INTO tb01  SELECT nationkey, name, regionkey, comment, name as part_key FROM nation;
INSERT INTO tb01  SELECT nationkey, name, regionkey, comment, name as part_key FROM nation;

CREATE TABLE tb02 (
    n_nationkey bigint,
    n_name varchar,
    n_regionkey bigint,
    n_comment varchar
) WITH (bucket_count = 10, bucketed_by = ARRAY['n_regionkey']);

INSERT INTO tb02  SELECT * FROM nation;


CREATE TABLE tb03 (
    n_nationkey bigint,
    n_name varchar,
    n_regionkey bigint,
    n_comment varchar
) WITH (bucket_count = 2, bucketed_by = ARRAY['n_regionkey'],sorted_by = ARRAY['n_regionkey']);

INSERT INTO tb03  SELECT * FROM nation;


CREATE TABLE tb04 (
    n_nationkey bigint,
    n_name varchar,
    n_regionkey bigint,
    n_comment varchar,
    part_key1 varchar,
    part_key2 bigint
) WITH (partitioned_by = ARRAY['part_key1','part_key2']);

INSERT INTO tb04  SELECT nationkey, name, regionkey, comment, name as part_key1,regionkey as part_key2  FROM nation;
INSERT INTO tb04  SELECT nationkey, name, regionkey, comment, name as part_key1,regionkey as part_key2  FROM nation;

SELECT count(*) FROM tb01 WHERE n_regionkey=0;

SELECT count(*) FROM tb01 WHERE part_key='ALGERIA';

SELECT count(*) FROM tb01 WHERE n_regionkey=0 AND part_key='ALGERIA';

SELECT count(*) FROM tb02 WHERE n_regionkey=0;

SELECT count(*)  FROM tb03;

select count(*) from tb04;
