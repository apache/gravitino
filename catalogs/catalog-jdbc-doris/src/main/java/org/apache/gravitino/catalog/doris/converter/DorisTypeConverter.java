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
package org.apache.gravitino.catalog.doris.converter;

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Type converter for Apache Doris. */
public class DorisTypeConverter extends JdbcTypeConverter {
  static final String BOOLEAN = "boolean";
  static final String TINYINT = "tinyint";
  static final String SMALLINT = "smallint";
  static final String INT = "int";
  static final String BIGINT = "bigint";
  static final String FLOAT = "float";
  static final String DOUBLE = "double";
  static final String DECIMAL = "decimal";
  static final String DATETIME = "datetime";
  static final String CHAR = "char";
  static final String STRING = "string";
  static final String BINARY = "binary";
  static final String VARBINARY = "varbinary";
  static final String JSON = "json";
  static final String VARIANT = "variant";
  static final String IPV4 = "ipv4";
  static final String IPV6 = "ipv6";
  static final String LARGEINT = "largeint";
  static final String BITMAP = "bitmap";
  static final String HLL = "hll";
  static final String DATEV2 = "datev2";
  static final String ARRAY = "array";
  static final String MAP = "map";
  static final String STRUCT = "struct";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    String typeName = typeBean.getTypeName().toLowerCase();

    // Extract base type name by stripping parenthesized parameters.
    // SHOW CREATE TABLE returns full type strings like "int(11)", "decimal(10,2)",
    // but the switch matches base names like "int", "decimal".
    String baseType = typeName;
    int parenIndex = typeName.indexOf('(');
    if (parenIndex > 0) {
      baseType = typeName.substring(0, parenIndex);
    }

    // Handle datetime(N) format — parse precision from type string when not in typeBean
    if ("datetime".equals(baseType)) {
      if (typeBean.getDatetimePrecision() != null) {
        return Types.TimestampType.withoutTimeZone(typeBean.getDatetimePrecision());
      }
      if (parenIndex > 0) {
        try {
          String precisionStr = typeName.substring(parenIndex + 1, typeName.length() - 1);
          int precision = Integer.parseInt(precisionStr);
          return Types.TimestampType.withoutTimeZone(precision);
        } catch (NumberFormatException e) {
          // Fall through to default datetime handling
        }
      }
      return Types.TimestampType.withoutTimeZone();
    }

    switch (baseType) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case TINYINT:
        return Types.ByteType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case INT:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DECIMAL:
        {
          // Parse precision/scale from type string when typeBean doesn't have them
          int columnSize = typeBean.getColumnSize();
          int scale = typeBean.getScale();
          if (columnSize == 0 && parenIndex > 0) {
            String[] parts = typeName.substring(parenIndex + 1, typeName.length() - 1).split(",");
            columnSize = Integer.parseInt(parts[0].trim());
            scale = parts.length >= 2 ? Integer.parseInt(parts[1].trim()) : 0;
          }
          return Types.DecimalType.of(columnSize, scale);
        }
      case DATE:
      case DATEV2:
        return Types.DateType.get();
      case DATETIME:
        // Already handled above the switch; this is a fallback
        return Types.TimestampType.withoutTimeZone();
      case CHAR:
        {
          int columnSize = typeBean.getColumnSize();
          if (columnSize == 0 && parenIndex > 0) {
            columnSize =
                Integer.parseInt(typeName.substring(parenIndex + 1, typeName.length() - 1));
          }
          return Types.FixedCharType.of(columnSize > 0 ? columnSize : 1);
        }
      case VARCHAR:
        {
          int columnSize = typeBean.getColumnSize();
          if (columnSize == 0 && parenIndex > 0) {
            columnSize =
                Integer.parseInt(typeName.substring(parenIndex + 1, typeName.length() - 1));
          }
          return Types.VarCharType.of(columnSize > 0 ? columnSize : 255);
        }
      case STRING:
      case TEXT:
        return Types.StringType.get();
      case BINARY:
      case VARBINARY:
        return Types.BinaryType.get();
      case "bigint unsigned":
      case JSON:
      case VARIANT:
      case IPV4:
      case IPV6:
      case LARGEINT:
      case BITMAP:
      case HLL:
        return Types.ExternalType.of(typeName);
      default:
        return toGravitinoComplexType(typeBean);
    }
  }

  /**
   * Handle complex types (ARRAY, MAP, STRUCT) whose TYPE_NAME includes nested type parameters, e.g.
   * "array&lt;text&gt;", "map&lt;text,int&gt;", "struct&lt;x:int,y:int&gt;". These cannot be
   * matched by simple case labels.
   */
  private static Type toGravitinoComplexType(JdbcTypeBean typeBean) {
    String typeName = typeBean.getTypeName().toLowerCase();

    if (typeName.startsWith("array<") && typeName.endsWith(">")) {
      // Parse ARRAY type: array<element_type>
      String elementTypeStr = typeName.substring(6, typeName.length() - 1);
      Type elementType = parseSimpleType(elementTypeStr);
      return Types.ListType.of(elementType, true);
    } else if (typeName.startsWith("map<") && typeName.endsWith(">")) {
      // Parse MAP type: map<key_type,value_type>
      String mapContent = typeName.substring(4, typeName.length() - 1);
      int commaIndex = findCommaIndex(mapContent);
      if (commaIndex > 0) {
        String keyTypeStr = mapContent.substring(0, commaIndex).trim();
        String valueTypeStr = mapContent.substring(commaIndex + 1).trim();
        Type keyType = parseSimpleType(keyTypeStr);
        Type valueType = parseSimpleType(valueTypeStr);
        return Types.MapType.of(keyType, valueType, true);
      }
    } else if (typeName.startsWith("struct<") && typeName.endsWith(">")) {
      // Parse STRUCT type: struct<field1:type1,field2:type2>
      // Use depth-aware split to handle nested types like struct<a:decimal(10,2),b:int>
      String structContent = typeName.substring(7, typeName.length() - 1);
      List<Types.StructType.Field> structFields = new ArrayList<>();
      int start = 0;
      while (start <= structContent.length()) {
        int comma = findCommaIndex(structContent.substring(start));
        int end = comma < 0 ? structContent.length() : start + comma;
        String field = structContent.substring(start, end).trim();
        int colonIndex = field.indexOf(':');
        if (colonIndex > 0) {
          String fieldName = field.substring(0, colonIndex).trim();
          String fieldTypeStr = field.substring(colonIndex + 1).trim();
          Type fieldType = parseSimpleType(fieldTypeStr);
          structFields.add(Types.StructType.Field.of(fieldName, fieldType, true, null));
        }
        if (comma < 0) break;
        start += comma + 1;
      }
      if (!structFields.isEmpty()) {
        return Types.StructType.of(structFields.toArray(new Types.StructType.Field[0]));
      }
    }

    // Fallback to ExternalType for unknown complex types
    return Types.ExternalType.of(typeBean.getTypeName());
  }

  /**
   * Find the index of the comma that separates key and value types in MAP type, or fields in
   * STRUCT. Handles nested types like "map&lt;struct&lt;a:int&gt;,int&gt;" and decimal(10,2) by
   * tracking both angle-bracket and parenthesis depth.
   */
  private static int findCommaIndex(String s) {
    int depth = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '<' || c == '(') {
        depth++;
      } else if (c == '>' || c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        return i;
      }
    }
    return -1;
  }

  /** Parse simple type string to Gravitino Type. */
  private static Type parseSimpleType(String typeStr) {
    // Handle types with precision like int(11), bigint(20)
    String baseType = typeStr;
    int parenIndex = typeStr.indexOf('(');
    if (parenIndex > 0) {
      baseType = typeStr.substring(0, parenIndex);
    }

    switch (baseType) {
      case "boolean":
        return Types.BooleanType.get();
      case "tinyint":
        return Types.ByteType.get();
      case "smallint":
        return Types.ShortType.get();
      case "int":
      case "integer":
        return Types.IntegerType.get();
      case "bigint":
        return Types.LongType.get();
      case "float":
        return Types.FloatType.get();
      case "double":
        return Types.DoubleType.get();
      case "date":
      case "datev2":
        return Types.DateType.get();
      case "datetime":
        if (parenIndex > 0) {
          String precisionStr = typeStr.substring(parenIndex + 1, typeStr.length() - 1);
          int precision = Integer.parseInt(precisionStr);
          return Types.TimestampType.withoutTimeZone(precision);
        }
        return Types.TimestampType.withoutTimeZone();
      case "string":
      case "text":
        return Types.StringType.get();
      case "binary":
      case "varbinary":
        return Types.BinaryType.get();
      case "varchar":
        if (parenIndex > 0) {
          int length = Integer.parseInt(typeStr.substring(parenIndex + 1, typeStr.length() - 1));
          return Types.VarCharType.of(length);
        }
        return Types.VarCharType.of(255);
      case "char":
        if (parenIndex > 0) {
          int length = Integer.parseInt(typeStr.substring(parenIndex + 1, typeStr.length() - 1));
          return Types.FixedCharType.of(length);
        }
        return Types.FixedCharType.of(1);
      case "decimal":
        if (parenIndex > 0) {
          String[] parts = typeStr.substring(parenIndex + 1, typeStr.length() - 1).split(",");
          int precision = Integer.parseInt(parts[0].trim());
          int scale = parts.length >= 2 ? Integer.parseInt(parts[1].trim()) : 0;
          return Types.DecimalType.of(precision, scale);
        }
        return Types.DecimalType.of(10, 0);
      default:
        // Unknown nested type — preserve as external type rather than silently mapping to String.
        // TODO: recursively resolve nested complex types (e.g. array<array<int>>, map<string,
        //   array<int>>) by delegating to toGravitinoComplexType instead of falling back here.
        return Types.ExternalType.of(typeStr);
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.BooleanType) {
      return BOOLEAN;
    } else if (type instanceof Types.ByteType) {
      return TINYINT;
    } else if (type instanceof Types.ShortType) {
      return SMALLINT;
    } else if (type instanceof Types.IntegerType) {
      return INT;
    } else if (type instanceof Types.LongType) {
      return BIGINT;
    } else if (type instanceof Types.FloatType) {
      return FLOAT;
    } else if (type instanceof Types.DoubleType) {
      return DOUBLE;
    } else if (type instanceof Types.DecimalType) {
      return DECIMAL
          + "("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.DateType) {
      return DATEV2;
    } else if (type instanceof Types.TimestampType) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      return timestampType.hasPrecisionSet()
          ? String.format("%s(%d)", DATETIME, timestampType.precision())
          : DATETIME;
    } else if (type instanceof Types.VarCharType) {
      int length = ((Types.VarCharType) type).length();
      if (length < 1 || length > 65533) {
        throw new IllegalArgumentException(
            String.format(
                "Type %s is invalid, length should be between 1 and 65533", type.simpleString()));
      }
      return VARCHAR + "(" + ((Types.VarCharType) type).length() + ")";
    } else if (type instanceof Types.FixedCharType) {
      int length = ((Types.FixedCharType) type).length();
      if (length < 1 || length > 255) {
        throw new IllegalArgumentException(
            String.format(
                "Type %s is invalid, length should be between 1 and 255", type.simpleString()));
      }

      return CHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.StringType) {
      return STRING;
    } else if (type instanceof Types.BinaryType) {
      return BINARY;
    } else if (type instanceof Types.ListType) {
      Types.ListType listType = (Types.ListType) type;
      return ARRAY + "<" + fromGravitino(listType.elementType()) + ">";
    } else if (type instanceof Types.MapType) {
      Types.MapType mapType = (Types.MapType) type;
      return MAP
          + "<"
          + fromGravitino(mapType.keyType())
          + ","
          + fromGravitino(mapType.valueType())
          + ">";
    } else if (type instanceof Types.StructType) {
      Types.StructType structType = (Types.StructType) type;
      StringBuilder sb = new StringBuilder(STRUCT + "<");
      for (int i = 0; i < structType.fields().length; i++) {
        if (i > 0) sb.append(",");
        Types.StructType.Field field = structType.fields()[i];
        sb.append(field.name()).append(":").append(fromGravitino(field.type()));
      }
      sb.append(">");
      return sb.toString();
    }
    throw new IllegalArgumentException(
        String.format("Couldn't convert Gravitino type %s to Doris type", type.simpleString()));
  }
}
