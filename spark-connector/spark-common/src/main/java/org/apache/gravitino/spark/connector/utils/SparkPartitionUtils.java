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

package org.apache.gravitino.spark.connector.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkPartitionUtils {

  private SparkPartitionUtils() {}

  public static Literal<?> toGravitinoLiteral(InternalRow ident, int ordinal, DataType sparkType) {
    if (ident.isNullAt(ordinal)) {
      return Literals.NULL;
    }

    if (sparkType instanceof ByteType) {
      return Literals.byteLiteral(ident.getByte(ordinal));
    } else if (sparkType instanceof ShortType) {
      return Literals.shortLiteral(ident.getShort(ordinal));
    } else if (sparkType instanceof IntegerType) {
      return Literals.integerLiteral(ident.getInt(ordinal));
    } else if (sparkType instanceof LongType) {
      return Literals.longLiteral(ident.getLong(ordinal));
    } else if (sparkType instanceof FloatType) {
      return Literals.floatLiteral(ident.getFloat(ordinal));
    } else if (sparkType instanceof DoubleType) {
      return Literals.doubleLiteral(ident.getDouble(ordinal));
    } else if (sparkType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) sparkType;
      org.apache.spark.sql.types.Decimal decimal =
          ident.getDecimal(ordinal, decimalType.precision(), decimalType.scale());
      return Literals.decimalLiteral(
          org.apache.gravitino.rel.types.Decimal.of(decimal.toJavaBigDecimal()));
    } else if (sparkType instanceof StringType) {
      return Literals.stringLiteral(ident.getString(ordinal));
    } else if (sparkType instanceof VarcharType) {
      VarcharType varcharType = (VarcharType) sparkType;
      return Literals.varcharLiteral(varcharType.length(), ident.getString(ordinal));
    } else if (sparkType instanceof CharType) {
      CharType charType = (CharType) sparkType;
      return Literals.of(ident.get(ordinal, sparkType), Types.FixedCharType.of(charType.length()));
    } else if (sparkType instanceof BooleanType) {
      return Literals.booleanLiteral(ident.getBoolean(ordinal));
    } else if (sparkType instanceof DateType) {
      LocalDate localDate = LocalDate.ofEpochDay(ident.getInt(ordinal));
      return Literals.dateLiteral(localDate);
    }
    throw new UnsupportedOperationException("Not support " + sparkType.toString());
  }

  public static String getPartitionValueAsString(
      InternalRow ident, int ordinal, DataType dataType) {
    if (ident.isNullAt(ordinal)) {
      return null;
    }
    if (dataType instanceof ByteType) {
      return String.valueOf(ident.getByte(ordinal));
    } else if (dataType instanceof ShortType) {
      return String.valueOf(ident.getShort(ordinal));
    } else if (dataType instanceof IntegerType) {
      return String.valueOf(ident.getInt(ordinal));
    } else if (dataType instanceof StringType) {
      return ident.getUTF8String(ordinal).toString();
    } else if (dataType instanceof VarcharType) {
      return ident.get(ordinal, dataType).toString();
    } else if (dataType instanceof CharType) {
      return ident.get(ordinal, dataType).toString();
    } else if (dataType instanceof DateType) {
      // DateType spark use int store.
      LocalDate localDate = LocalDate.ofEpochDay(ident.getInt(ordinal));
      return localDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
    } else if (dataType instanceof BooleanType) {
      return String.valueOf(ident.getBoolean(ordinal));
    } else if (dataType instanceof LongType) {
      return String.valueOf(ident.getLong(ordinal));
    } else if (dataType instanceof DoubleType) {
      return String.valueOf(ident.getDouble(ordinal));
    } else if (dataType instanceof FloatType) {
      return String.valueOf(ident.getFloat(ordinal));
    } else if (dataType instanceof DecimalType) {
      return ident
          .getDecimal(
              ordinal, ((DecimalType) dataType).precision(), ((DecimalType) dataType).scale())
          .toString();
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported partition column type: %s", dataType));
    }
  }

  public static Object getSparkPartitionValue(String hivePartitionValue, DataType dataType) {
    if (hivePartitionValue == null) {
      return null;
    }
    try {
      if (dataType instanceof ByteType) {
        return Byte.valueOf(hivePartitionValue);
      } else if (dataType instanceof ShortType) {
        return Short.valueOf(hivePartitionValue);
      } else if (dataType instanceof IntegerType) {
        return Integer.parseInt(hivePartitionValue);
      } else if (dataType instanceof LongType) {
        return Long.parseLong(hivePartitionValue);
      } else if (dataType instanceof StringType) {
        return UTF8String.fromString(hivePartitionValue);
      } else if (dataType instanceof DateType) {
        LocalDate localDate = LocalDate.parse(hivePartitionValue);
        // DateType spark use int store.
        return (int) localDate.toEpochDay();
      } else if (dataType instanceof BooleanType) {
        return Boolean.parseBoolean(hivePartitionValue);
      } else if (dataType instanceof DoubleType) {
        return Double.parseDouble(hivePartitionValue);
      } else if (dataType instanceof FloatType) {
        return Float.parseFloat(hivePartitionValue);
      } else if (dataType instanceof DecimalType) {
        return Decimal.apply(hivePartitionValue);
      } else if (dataType instanceof VarcharType) {
        return UTF8String.fromString(hivePartitionValue);
      } else if (dataType instanceof CharType) {
        return UTF8String.fromString(hivePartitionValue);
      } else {
        throw new UnsupportedOperationException("Unsupported partition type: " + dataType);
      }
    } catch (Exception e) {
      throw new UnsupportedOperationException(
          String.format(
              "Failed to convert partition value '%s' to type %s", hivePartitionValue, dataType),
          e);
    }
  }
}
