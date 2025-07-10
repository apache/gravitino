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

package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLDataTypeTransformer.JSON_TYPE;

import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

/**
 * External type mapping between MySQL and Trino
 *
 * <p>TODO: type mapping base on jdbcType rather than typeName
 *
 * <p>see https://trino.io/docs/current/connector/mysql.html#mysql-to-trino-type-mapping
 */
public enum MySQLExternalDataType {
  MEDIUMINT("mediumint", IntegerType.INTEGER),
  MEDIUMINT_UNSIGNED("mediumint unsigned", IntegerType.INTEGER),
  FLOAT_UNSIGNED("float unsigned", RealType.REAL),
  DOUBLE_UNSIGNED("double unsigned", DoubleType.DOUBLE),
  TINYTEXT("tinytext", VarcharType.VARCHAR),
  MEDIUMTEXT("mediumtext", VarcharType.VARCHAR),
  LONGTEXT("longtext", VarcharType.VARCHAR),
  YEAR("year", DateType.DATE),
  ENUM("enum", VarcharType.VARCHAR),
  SET("set", VarcharType.VARCHAR),
  JSON("json", JSON_TYPE),
  VARBINARY("varbinary", VarbinaryType.VARBINARY),
  TINYBLOB("tinyblob", VarbinaryType.VARBINARY),
  BLOB("blob", VarbinaryType.VARBINARY),
  MEDIUMBLOB("mediumblob", VarbinaryType.VARBINARY),
  LONGBLOB("longblob", VarbinaryType.VARBINARY),
  GEOMETRY("geometry", VarbinaryType.VARBINARY),
  /**
   * In Trino, session property unsupported_type_handling default value is
   * UnsupportedTypeHandling.IGNORE. For unsupported data types, standardize mapping to varchar for
   * enhanced usability. Equivalent to default configuration of the unsupported_type_handling is
   * UnsupportedTypeHandling.CONVERT_TO_VARCHAR
   *
   * <p>TODO: type mapping support unsupported_type_handling and jdbc-types-mapped-to-varchar
   */
  UNKNOWN("unknown", VarcharType.VARCHAR);

  private final String mysqlTypeName;

  // suppress ImmutableEnumChecker because Type is outside the project.
  @SuppressWarnings("ImmutableEnumChecker")
  private final Type trinoType;

  private MySQLExternalDataType(String mysqlTypeName, Type trinoType) {
    this.mysqlTypeName = mysqlTypeName;
    this.trinoType = trinoType;
  }

  public String getMysqlTypeName() {
    return mysqlTypeName;
  }

  public Type getTrinoType() {
    return trinoType;
  }

  public static MySQLExternalDataType safeValueOf(String mysqlTypeName) {
    for (MySQLExternalDataType mySQLExternalDataType : MySQLExternalDataType.values()) {
      if (mySQLExternalDataType.mysqlTypeName.equalsIgnoreCase(mysqlTypeName)) {
        return mySQLExternalDataType;
      }
    }
    return UNKNOWN;
  }
}
