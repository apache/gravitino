-- Table create by integration-test-common/docker-script/init/mysql/init.sql
SHOW CREATE TABLE gt_mysql.gt_mysql_test_column_properties.demo_all_data_types_default_value;

SHOW CREATE TABLE gt_mysql.gt_mysql_test_column_properties.demo_default_value_with_expression;

CREATE SCHEMA gt_mysql.gt_mysql_test_column_properties_v2;

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_default_value(
    key1 INT NOT NULL WITH (auto_increment=true),
    f1 VARCHAR(200) WITH (default='VARCHAR'),
    f2 CHAR(20) WITH (default='CHAR') ,
    f4 DECIMAL(10, 3) WITH (default='0.3') ,
    f5 REAL WITH (default='0.3') ,
    f6 DOUBLE WITH (default='0.3') ,
    f8 TINYINT WITH (default='1') ,
    f9 SMALLINT WITH (default='1') ,
    f10 INT WITH (default='1') ,
    f11 INTEGER WITH (default='1') ,
    f12 BIGINT WITH (default='1'),
    f13 DATE WITH (default='2024-04-01'),
    f14 TIME WITH (default='08:00:00'),
    f15 TIMESTAMP WITH (default='2012-12-31 11:30:45'),
    f16 TIMESTAMP WITH TIME ZONE WITH (default='2012-12-31 11:30:45'),
    f17 TIMESTAMP WITH TIME ZONE WITH (default='CURRENT_TIMESTAMP')
)
WITH (
   primary_key = ARRAY['key1']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_default_value;

DROP TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_default_value;

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_invalid_default_value(
    key1 INT NOT NULL,
    f2 DATE WITH (default='1')
)
WITH (
   primary_key = ARRAY['key1']
);

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_invalid_default_value_2(
    key1 INT NOT NULL,
    f2 UUID WITH (default='1')
)
WITH (
   primary_key = ARRAY['key1']
);

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_1(
    key1 INT NOT NULL WITH (auto_increment=true),
    f1 TIMESTAMP ,
    f2 DATE
)
WITH (
   primary_key = ARRAY['key1']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_1;

DROP TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_1;

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_2(
    key1 INT NOT NULL WITH (auto_increment=true),
    key2 INT NOT NULL,
    f1 TIMESTAMP ,
    f2 DATE
)
WITH (
   primary_key = ARRAY['key1', 'key2']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_2;

DROP TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_2;


CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_3 (
   key1 INT NOT NULL WITH (auto_increment=true),
   col1 INT
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   unique_key = ARRAY['unique_key1:key1']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_3;

DROP TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_3;

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_4 (
   key1 INT NOT NULL WITH (auto_increment=true),
   key2 INT,
   col1 INT
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key1'],
   unique_key = ARRAY['unique_key1:key1,key2']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_4;

DROP TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_auto_increment_4;

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_invalid_auto_increment_1 (
   key1 INT NOT NULL WITH (auto_increment=true),
   key2 INT,
   col1 INT
)
COMMENT ''
WITH (
   engine = 'InnoDB'
);

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_invalid_auto_increment_2 (
   key1 INT NOT NULL WITH (auto_increment=true),
   key2 INT NOT NULL WITH (auto_increment=true),
   col1 INT
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key2','key1']
);

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_invalid_auto_increment_3 (
   key1 INT WITH (auto_increment=true),
   col1 INT
)
COMMENT ''
WITH (
   engine = 'InnoDB',
    unique_key = ARRAY['unique_key1:key1']
);

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_invalid_auto_increment_4 (
   key1 INT NOT NULL WITH (auto_increment=true, default='1'),
   col1 INT
)
COMMENT ''
WITH (
   engine = 'InnoDB',
    unique_key = ARRAY['unique_key1:key1']
);

CREATE TABLE gt_mysql.gt_mysql_test_column_properties_v2.test_create_with_invalid_auto_increment_5 (
   key1 VARCHAR NOT NULL WITH (auto_increment=true),
   col1 INT
)
COMMENT ''
WITH (
   engine = 'InnoDB',
    unique_key = ARRAY['unique_key1:key1']
);

DROP SCHEMA gt_mysql.gt_mysql_test_column_properties_v2;