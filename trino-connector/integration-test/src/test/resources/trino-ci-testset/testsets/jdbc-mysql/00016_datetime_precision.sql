-- Table create by integration-test-common/docker-script/init/mysql/init.sql
show create table gt_mysql.gt_mysql_test_all_type.demo_datetime_precision;

select * from gt_mysql.gt_mysql_test_all_type.demo_datetime_precision;

CREATE TABLE gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_v2 (
    id INT NOT NULL,
    time_col TIME,
    time_col_6 TIME(6),
    datetime_col TIMESTAMP,
    datetime_col_6 TIMESTAMP(6),
    datetime_col_9 TIMESTAMP(9),
    timestamp_col TIMESTAMP WITH TIME ZONE,
    timestamp_col_6 TIMESTAMP(6) WITH TIME ZONE WITH (default='CURRENT_TIMESTAMP')
)
WITH (
   primary_key = ARRAY['id']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_v2;

INSERT INTO gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_v2
VALUES (1, TIME '14:30:00.123', TIME '14:30:00.123456', TIMESTAMP '2025-07-04 14:30:00.123', TIMESTAMP '2025-07-04 14:30:00.123456', TIMESTAMP '2025-07-04 14:30:00.123456', TIMESTAMP '2025-07-04 14:30:00.123 UTC', TIMESTAMP '2025-07-04 14:30:00.123456 UTC'),
       (2, TIME '23:59:59.999', TIME '23:59:59.999999', TIMESTAMP '2025-12-31 23:59:59.999', TIMESTAMP '2025-12-31 23:59:59.999999', TIMESTAMP '2025-12-31 23:59:59.999999', TIMESTAMP '2025-12-31 23:59:59.999 UTC', TIMESTAMP '2025-12-31 23:59:59.999999 UTC');

SELECT * FROM gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_v2 ORDER BY id;

CREATE TABLE gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_ctas AS SELECT * FROM gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_v2;

SELECT * FROM gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_ctas ORDER BY id;

DROP TABLE gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_ctas;

CREATE TABLE gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_ctas_9 AS
SELECT TIME '14:30:00.123456789' AS time_col_9, TIMESTAMP '2025-07-04 14:30:00.123456789' AS datetime_col_9, TIMESTAMP '2025-07-04 14:30:00.123456789 UTC' AS timestamp_col_9;

SELECT * FROM gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_ctas_9;

DROP TABLE gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_ctas_9;

DROP TABLE gt_mysql.gt_mysql_test_all_type.demo_datetime_precision_v2;
