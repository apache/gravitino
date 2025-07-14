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

