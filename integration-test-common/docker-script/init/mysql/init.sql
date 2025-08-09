/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
GRANT ALL PRIVILEGES on *.* to 'trino'@'%';
FLUSH PRIVILEGES;
CREATE DATABASE gt_mysql_test_all_type;
CREATE TABLE gt_mysql_test_all_type.demo
(
    -- 数值类型
    id BIGINT PRIMARY KEY,
    tiny_col TINYINT,
    tiny_unsigned_col TINYINT UNSIGNED,
    small_col SMALLINT,
    small_unsigned_col SMALLINT UNSIGNED,
    medium_col MEDIUMINT,
    medium_unsigned_col MEDIUMINT UNSIGNED,
    int_col INT,
    int_unsigned_col INT UNSIGNED,
    bigint_col BIGINT,
    bigint_unsigned_col BIGINT UNSIGNED,
    float_col FLOAT,
    float_unsigned_col FLOAT UNSIGNED,
    double_col DOUBLE,
    double_unsigned_col DOUBLE UNSIGNED,
    decimal_col DECIMAL(12,6),
    decimal_unsigned_col DECIMAL(12,6) UNSIGNED,
    -- 字符串类型
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    tinytext_col TINYTEXT,
    text_col TEXT,
    mediumtext_col MEDIUMTEXT,
    longtext_col LONGTEXT,
    -- 日期时间类型
    date_col DATE,
    time_col TIME,
    datetime_col DATETIME,
    timestamp_col TIMESTAMP,
    year_col YEAR,
    -- json
    json_col JSON,
    -- 枚举与集合
    enum_col ENUM('red','green','blue'),
    set_col SET('read','write','execute'),
    -- 二进制类型
    binary_col BINARY(16),
    varbinary_col VARBINARY(100),
    tinyblob_col TINYBLOB,
    blob_col BLOB,
    mediumblob_col MEDIUMBLOB,
    longblob_col LONGBLOB,
    -- 空间类型
    point_col POINT,
    geometry_col GEOMETRY
);
INSERT INTO gt_mysql_test_all_type.demo (
    id, tiny_col, tiny_unsigned_col, small_col, small_unsigned_col,medium_col,
    medium_unsigned_col, int_col, int_unsigned_col, bigint_col, bigint_unsigned_col,
    float_col, float_unsigned_col, double_col, double_unsigned_col, decimal_col, decimal_unsigned_col,
    char_col, varchar_col, tinytext_col, text_col, mediumtext_col, longtext_col,
    date_col, time_col, datetime_col, timestamp_col, year_col, json_col,
    enum_col, set_col,
    binary_col, varbinary_col, tinyblob_col, blob_col, mediumblob_col, longblob_col,
    point_col, geometry_col
) VALUES (
    1,                                      -- id
    100,                                    -- tiny_col
    100,                                    -- tiny_unsigned_col
    500,                                    -- small_col
    500,                                    -- small_unsigned_col
    10000,                                  -- medium_col
    10000,                                  -- medium_unsigned_col
    200000,                                 -- int_col
    200000,                                 -- int_unsigned_col
    200000,                                 -- bigint_col
    200000,                                 -- bigint_unsigned_col
    123.45,                                 -- float_col
    123.45,                                 -- float_unsigned_col
    9876.5432,                              -- double_col
    9876.5432,                              -- double_unsigned_col
    123456.789000,                          -- decimal_col
    123456.789000,                          -- decimal_unsigned_col
    'abc',                                  -- char_col
    'abc',                                  -- varchar_col
    'abc',                                  -- tinytext_col
    'abc',                                  -- text_col
    'abc',                                  -- mediumtext_col
    'abc',                                  -- longtext_col
    '2025-07-04',                           -- date_col
    '14:30:00',                             -- time_col
    '2025-07-04 14:30:00',                  -- datetime_col
    '2025-07-04 14:30:00',                  -- timestamp_col
    '2025',                                 -- year_col
    '{"x": 1, "y": 2}',                     -- json_col
    'green',                                -- enum_col
    'read,write',                           -- set_col
    X'A1B2C3D4',                            -- binary_col
    X'A1B2',                                -- varbinary_col
    X'123456',                              -- tinyblob_col
    X'ABCDEF',                              -- blob_col
    X'1234567890',                          -- mediumblob_col
    X'A1B2C3D4E5',                          -- longblob_col
    ST_GeomFromText('POINT(10 20)'),        -- point_col
    ST_GeomFromText('LINESTRING(0 0, 10 10)') -- geometry_col
);
CREATE DATABASE gt_mysql_test_column_properties;
/*
+----------------+-------------------------------------+------+-----+---------------------+-------------------+
| Field          | Type                                | Null | Key | Default             | Extra             |
+----------------+-------------------------------------+------+-----+---------------------+-------------------+
| col_bit        | bit(1)                              | YES  |     | b'1'                |                   |
| col_tinyint    | tinyint                             | YES  |     | 0                   |                   |
| col_smallint   | smallint                            | YES  |     | -1                  |                   |
| col_mediumint  | mediumint                           | YES  |     | 100                 |                   |
| col_int        | int                                 | YES  |     | 1                   |                   |
| col_bigint     | bigint                              | YES  |     | 9999999             |                   |
| col_float      | float                               | YES  |     | 0                   |                   |
| col_double     | double                              | YES  |     | 1.23456             |                   |
| col_decimal    | decimal(10,2)                       | YES  |     | 100.50              |                   |
| col_date       | date                                | YES  |     | 2000-01-01          |                   |
| col_time       | time                                | YES  |     | 12:30:01            |                   |
| col_datetime   | datetime                            | YES  |     | 2025-01-01 00:00:01 |                   |
| col_timestamp  | timestamp                           | NO   |     | 2025-01-01 00:00:01 |                   |
| col_year       | year                                | YES  |     | 2025                |                   |
| col_char       | char(10)                            | YES  |     | abc                 |                   |
| col_varchar    | varchar(100)                        | YES  |     | Hello               |                   |
| col_tinytext   | tinytext                            | YES  |     | NULL                |                   |
| col_text       | text                                | YES  |     | NULL                |                   |
| col_mediumtext | mediumtext                          | YES  |     | NULL                |                   |
| col_longtext   | longtext                            | YES  |     | NULL                |                   |
| col_binary     | binary(5)                           | YES  |     | 0x61                |                   |
| col_varbinary  | varbinary(100)                      | YES  |     | 0x62696E617279      |                   |
| col_blob       | blob                                | YES  |     | NULL                |                   |
| col_mediumblob | mediumblob                          | YES  |     | NULL                |                   |
| col_longblob   | longblob                            | YES  |     | NULL                |                   |
| col_enum       | enum('active','inactive','pending') | YES  |     | pending             |                   |
| col_set        | set('red','green','blue')           | YES  |     | green,blue          |                   |
| col_json       | json                                | YES  |     | NULL                |                   |
| col_boolean    | tinyint(1)                          | YES  |     | 1                   |                   |
| col_geometry   | geometry                            | YES  |     | NULL                |                   |
| col_point      | point                               | YES  |     | point(0,0)          | DEFAULT_GENERATED |
+----------------+-------------------------------------+------+-----+---------------------+-------------------+
*/
CREATE TABLE gt_mysql_test_column_properties.demo_all_data_types_default_value (
    -- 整数类型
    col_bit BIT DEFAULT b'1',
    col_tinyint TINYINT DEFAULT 0,
    col_smallint SMALLINT DEFAULT -1,
    col_mediumint MEDIUMINT DEFAULT 100,
    col_int INT DEFAULT 1,
    col_bigint BIGINT DEFAULT 9999999,
    -- 浮点与精确小数
    col_float FLOAT DEFAULT 0.0,
    col_double DOUBLE DEFAULT 1.23456,
    col_decimal DECIMAL(10, 2) DEFAULT 100.50,
    -- 日期与时间
    col_date DATE DEFAULT '2000-01-01',
    col_time TIME DEFAULT '12:30:01',
    col_datetime DATETIME DEFAULT '2025-01-01 00:00:01',
    col_timestamp TIMESTAMP DEFAULT '2025-01-01 00:00:01',
    col_year YEAR DEFAULT 2025,
    -- 字符串类型
    col_char CHAR(10) DEFAULT 'abc',
    col_varchar VARCHAR(100) DEFAULT 'Hello',
    col_tinytext TINYTEXT DEFAULT NULL,
    col_text TEXT DEFAULT NULL,
    col_mediumtext MEDIUMTEXT DEFAULT NULL,
    col_longtext LONGTEXT DEFAULT NULL,
    -- 二进制数据
    col_binary BINARY(5) DEFAULT 'a',
    col_varbinary VARBINARY(100) DEFAULT 'binary',
    col_blob BLOB DEFAULT NULL,
    col_mediumblob MEDIUMBLOB DEFAULT NULL,
    col_longblob LONGBLOB DEFAULT NULL,
    -- 特殊类型
    col_enum ENUM('active', 'inactive', 'pending') DEFAULT 'pending',
    col_set SET('red', 'green', 'blue') DEFAULT 'green,blue',
    col_json JSON DEFAULT NULL,
    col_boolean BOOLEAN DEFAULT TRUE,
    -- 空间数据类型
    col_geometry GEOMETRY DEFAULT NULL,
    col_point POINT DEFAULT (POINT(0, 0))
);
/*
+------------------+--------------+------+-----+-------------------------------+-------------------+
| Field            | Type         | Null | Key | Default                       | Extra             |
+------------------+--------------+------+-----+-------------------------------+-------------------+
| int_col_2        | int          | YES  |     | rand()                        | DEFAULT_GENERATED |
| varchar200_col_1 | varchar(200) | YES  |     | curdate()                     | DEFAULT_GENERATED |
| varchar200_col_2 | varchar(200) | YES  |     | now()                         | DEFAULT_GENERATED |
| datetime_col_1   | datetime     | YES  |     | CURRENT_TIMESTAMP             | DEFAULT_GENERATED |
| datetime_col_2   | datetime     | YES  |     | CURRENT_TIMESTAMP             | DEFAULT_GENERATED |
| date_col_1       | date         | YES  |     | curdate()                     | DEFAULT_GENERATED |
| date_col_2       | date         | YES  |     | (curdate() + interval 1 year) | DEFAULT_GENERATED |
| date_col_3       | date         | YES  |     | curdate()                     | DEFAULT_GENERATED |
| timestamp_col_1  | timestamp    | NO   |     | CURRENT_TIMESTAMP             | DEFAULT_GENERATED |
| timestamp_col_2  | timestamp(6) | NO   |     | CURRENT_TIMESTAMP(6)          | DEFAULT_GENERATED |
+------------------+--------------+------+-----+-------------------------------+-------------------+
*/
CREATE TABLE gt_mysql_test_column_properties.demo_default_value_with_expression
(
  int_col_2 int default (rand()),
  varchar200_col_1 varchar(200) default (curdate()),
  varchar200_col_2 varchar(200) default (CURRENT_TIMESTAMP),
  datetime_col_1 datetime default CURRENT_TIMESTAMP,
  datetime_col_2 datetime default current_timestamp,
  date_col_1 date default (CURRENT_DATE),
  date_col_2 date DEFAULT (CURRENT_DATE + INTERVAL 1 YEAR),
  date_col_3 date DEFAULT (CURRENT_DATE),
  timestamp_col_1 timestamp default CURRENT_TIMESTAMP,
  timestamp_col_2 timestamp(6) default CURRENT_TIMESTAMP(6)
);
