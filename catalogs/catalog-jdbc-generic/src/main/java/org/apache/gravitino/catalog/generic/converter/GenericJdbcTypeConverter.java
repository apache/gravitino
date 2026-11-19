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
package org.apache.gravitino.catalog.generic.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Conservative type converter based on standard JDBC type codes. */
public class GenericJdbcTypeConverter extends JdbcTypeConverter {

  /** JDBC type bean that also carries the standard {@code java.sql.Types} code. */
  public static class GenericJdbcTypeBean extends JdbcTypeBean {
    private final int jdbcType;

    /** Creates a generic JDBC type bean. */
    public GenericJdbcTypeBean(String typeName, int jdbcType) {
      super(typeName);
      this.jdbcType = jdbcType;
    }

    /** Returns the standard JDBC type code. */
    public int getJdbcType() {
      return jdbcType;
    }
  }

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    if (!(typeBean instanceof GenericJdbcTypeBean)) {
      return Types.ExternalType.of(typeBean.getTypeName());
    }

    GenericJdbcTypeBean genericTypeBean = (GenericJdbcTypeBean) typeBean;
    switch (genericTypeBean.getJdbcType()) {
      case java.sql.Types.BOOLEAN:
      case java.sql.Types.BIT:
        return Types.BooleanType.get();
      case java.sql.Types.TINYINT:
        return Types.ByteType.get();
      case java.sql.Types.SMALLINT:
        return Types.ShortType.get();
      case java.sql.Types.INTEGER:
        return Types.IntegerType.get();
      case java.sql.Types.BIGINT:
        return Types.LongType.get();
      case java.sql.Types.FLOAT:
      case java.sql.Types.REAL:
        return Types.FloatType.get();
      case java.sql.Types.DOUBLE:
        return Types.DoubleType.get();
      case java.sql.Types.DECIMAL:
      case java.sql.Types.NUMERIC:
        return toDecimalType(genericTypeBean);
      case java.sql.Types.CHAR:
      case java.sql.Types.NCHAR:
        return genericTypeBean.getColumnSize() == null
            ? Types.StringType.get()
            : Types.FixedCharType.of(genericTypeBean.getColumnSize());
      case java.sql.Types.VARCHAR:
      case java.sql.Types.NVARCHAR:
        return genericTypeBean.getColumnSize() == null
            ? Types.StringType.get()
            : Types.VarCharType.of(genericTypeBean.getColumnSize());
      case java.sql.Types.LONGVARCHAR:
      case java.sql.Types.LONGNVARCHAR:
        return Types.StringType.get();
      case java.sql.Types.DATE:
        return Types.DateType.get();
      case java.sql.Types.TIME:
      case java.sql.Types.TIME_WITH_TIMEZONE:
        return Types.TimeType.get();
      case java.sql.Types.TIMESTAMP:
        return Types.TimestampType.withoutTimeZone();
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
        return Types.TimestampType.withTimeZone();
      case java.sql.Types.BINARY:
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
      case java.sql.Types.BLOB:
        return Types.BinaryType.get();
      default:
        return Types.ExternalType.of(genericTypeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.BooleanType) {
      return "BOOLEAN";
    } else if (type instanceof Types.ByteType) {
      return "TINYINT";
    } else if (type instanceof Types.ShortType) {
      return "SMALLINT";
    } else if (type instanceof Types.IntegerType) {
      return "INTEGER";
    } else if (type instanceof Types.LongType) {
      return "BIGINT";
    } else if (type instanceof Types.FloatType) {
      return "REAL";
    } else if (type instanceof Types.DoubleType) {
      return "DOUBLE";
    } else if (type instanceof Types.DecimalType) {
      return type.simpleString();
    } else if (type instanceof Types.FixedCharType) {
      return type.simpleString();
    } else if (type instanceof Types.VarCharType) {
      return type.simpleString();
    } else if (type instanceof Types.StringType) {
      return "VARCHAR";
    } else if (type instanceof Types.DateType) {
      return "DATE";
    } else if (type instanceof Types.TimeType) {
      return "TIME";
    } else if (type instanceof Types.TimestampType) {
      return "TIMESTAMP";
    } else if (type instanceof Types.BinaryType) {
      return "VARBINARY";
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }

    throw new IllegalArgumentException(
        String.format(
            "Couldn't convert Gravitino type %s to generic JDBC type", type.simpleString()));
  }

  private Type toDecimalType(GenericJdbcTypeBean typeBean) {
    Integer precision = typeBean.getColumnSize();
    Integer scale = typeBean.getScale();
    if (precision == null || precision <= 0) {
      return Types.ExternalType.of(typeBean.getTypeName());
    }
    return Types.DecimalType.of(precision, scale == null ? 0 : scale);
  }
}
