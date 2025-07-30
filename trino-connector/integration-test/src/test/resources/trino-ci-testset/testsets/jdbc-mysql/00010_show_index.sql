CREATE SCHEMA gt_mysql.gt_mysql_test_index;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_one_primary_key (
   key1 integer NOT NULL,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key1']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_one_primary_key;

DROP TABLE gt_mysql.gt_mysql_test_index.demo_with_one_primary_key;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_two_primary_key (
   key1 integer NOT NULL,
   key2 integer NOT NULL,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key2','key1']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_two_primary_key;

DROP TABLE gt_mysql.gt_mysql_test_index.demo_with_two_primary_key;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_one_unique_key (
   key1 integer,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   unique_key = ARRAY['unique_key1:key1']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_one_unique_key;

DROP TABLE gt_mysql.gt_mysql_test_index.demo_with_one_unique_key;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_one_unique_key_1 (
   key1 integer,
   key2 integer,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   unique_key = ARRAY['unique_key1:key2,key1']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_one_unique_key_1;

DROP TABLE gt_mysql.gt_mysql_test_index.demo_with_one_unique_key_1;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_two_unique_key (
   key1 integer,
   key2 integer,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   unique_key = ARRAY['unique_key1:key1','unique_key2:key2']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_two_unique_key;

DROP TABLE gt_mysql.gt_mysql_test_index.demo_with_two_unique_key;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_two_unique_key_1 (
   key1 integer,
   key2 integer,
   key3 integer,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   unique_key = ARRAY['unique_key1:key1','unique_key2:key3,key2']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_two_unique_key_1;

DROP TABLE gt_mysql.gt_mysql_test_index.demo_with_two_unique_key_1;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_primary_key_and_unique_key (
   key1 integer NOT NULL,
   key2 integer,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key1'],
   unique_key = ARRAY['unique_key1:key2']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_primary_key_and_unique_key;

DROP TABLE gt_mysql.gt_mysql_test_index.demo_with_primary_key_and_unique_key;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_primary_key_and_unique_key_1 (
   key1 integer NOT NULL,
   key2 integer,
   key3 integer,
   key4 integer,
   key5 integer NOT NULL,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key5','key1'],
   unique_key = ARRAY['unique_key1:key2','unique_key2:key4,key3']
);

SHOW CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_primary_key_and_unique_key_1;

DROP TABLE gt_mysql.gt_mysql_test_index.demo_with_primary_key_and_unique_key_1;

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_invalid_primary_key (
   key1 integer NOT NULL,
   key2 integer,
   key3 integer,
   key4 integer,
   key5 integer,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key5','key1']
);

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_invalid_primary_key_1 (
   key1 integer NOT NULL,
   key2 integer,
   key3 integer,
   key4 integer,
   key5 integer,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   primary_key = ARRAY['key6','key1']
);

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_invalid_unique_key (
   key1 integer NOT NULL,
   key2 integer,
   key3 integer,
   key4 integer,
   key5 integer NOT NULL,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   unique_key = ARRAY['unique_key1:key7','unique_key2:key4,key3']
);

CREATE TABLE gt_mysql.gt_mysql_test_index.demo_with_invalid_unique_key (
   key1 integer NOT NULL,
   key2 integer,
   key3 integer,
   key4 integer,
   key5 integer NOT NULL,
   col1 integer
)
COMMENT ''
WITH (
   engine = 'InnoDB',
   unique_key = ARRAY['unique_key1:']
);

DROP SCHEMA gt_mysql.gt_mysql_test_index;
