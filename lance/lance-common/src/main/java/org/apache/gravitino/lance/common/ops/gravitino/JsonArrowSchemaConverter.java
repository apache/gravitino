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
package org.apache.gravitino.lance.common.ops.gravitino;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.namespace.model.JsonArrowDataType;
import org.lance.namespace.model.JsonArrowField;
import org.lance.namespace.model.JsonArrowSchema;

/** Converts Arrow schema to Lance Namespace JsonArrowSchema model. */
class JsonArrowSchemaConverter {

  private JsonArrowSchemaConverter() {}

  static JsonArrowSchema convertToJsonArrowSchema(Schema schema) {
    JsonArrowSchema jsonSchema = new JsonArrowSchema();
    jsonSchema.setFields(
        schema.getFields().stream()
            .map(JsonArrowSchemaConverter::toJsonArrowField)
            .collect(Collectors.toList()));
    if (schema.getCustomMetadata() != null && !schema.getCustomMetadata().isEmpty()) {
      jsonSchema.setMetadata(schema.getCustomMetadata());
    }
    return jsonSchema;
  }

  private static JsonArrowField toJsonArrowField(Field field) {
    JsonArrowField jsonField = new JsonArrowField();
    jsonField.setName(field.getName());
    jsonField.setNullable(field.isNullable());
    if (field.getMetadata() != null && !field.getMetadata().isEmpty()) {
      jsonField.setMetadata(field.getMetadata());
    }
    jsonField.setType(toJsonArrowDataType(field.getType(), field.getChildren()));
    return jsonField;
  }

  private static JsonArrowDataType toJsonArrowDataType(ArrowType type, List<Field> children) {
    JsonArrowDataType jsonDataType = new JsonArrowDataType();
    jsonDataType.setType(toTypeName(type));

    Long length = toLength(type);
    if (length != null) {
      jsonDataType.setLength(length);
    }

    if (children != null && !children.isEmpty()) {
      jsonDataType.setFields(
          children.stream()
              .map(JsonArrowSchemaConverter::toJsonArrowField)
              .collect(Collectors.toList()));
    }
    return jsonDataType;
  }

  private static Long toLength(ArrowType type) {
    if (type instanceof ArrowType.FixedSizeBinary) {
      return (long) ((ArrowType.FixedSizeBinary) type).getByteWidth();
    }
    if (type instanceof ArrowType.FixedSizeList) {
      return (long) ((ArrowType.FixedSizeList) type).getListSize();
    }
    return null;
  }

  private static String toTypeName(ArrowType type) {
    if (type instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) type;
      return (intType.getIsSigned() ? "int" : "uint") + intType.getBitWidth();
    }
    if (type instanceof ArrowType.FloatingPoint) {
      FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
      if (precision == FloatingPointPrecision.HALF) {
        return "float16";
      }
      if (precision == FloatingPointPrecision.SINGLE) {
        return "float32";
      }
      return "float64";
    }
    if (type instanceof ArrowType.Utf8) {
      return "utf8";
    }
    if (type instanceof ArrowType.LargeUtf8) {
      return "large_utf8";
    }
    if (type instanceof ArrowType.Bool) {
      return "bool";
    }
    if (type instanceof ArrowType.Binary) {
      return "binary";
    }
    if (type instanceof ArrowType.LargeBinary) {
      return "large_binary";
    }
    if (type instanceof ArrowType.FixedSizeBinary) {
      return "fixed_size_binary";
    }
    if (type instanceof ArrowType.Struct) {
      return "struct";
    }
    if (type instanceof ArrowType.List) {
      return "list";
    }
    if (type instanceof ArrowType.LargeList) {
      return "large_list";
    }
    if (type instanceof ArrowType.FixedSizeList) {
      return "fixed_size_list";
    }
    if (type instanceof ArrowType.Map) {
      return "map";
    }
    if (type instanceof ArrowType.Decimal) {
      return "decimal";
    }
    if (type instanceof ArrowType.Date) {
      ArrowType.Date dateType = (ArrowType.Date) type;
      if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.DAY) {
        return "date32";
      }
      return "date64";
    }
    if (type instanceof ArrowType.Time) {
      ArrowType.Time timeType = (ArrowType.Time) type;
      return timeType.getBitWidth() == 32 ? "time32" : "time64";
    }
    if (type instanceof ArrowType.Timestamp) {
      return "timestamp";
    }
    if (type instanceof ArrowType.Interval) {
      return "interval";
    }
    if (type instanceof ArrowType.Duration) {
      return "duration";
    }
    if (type instanceof ArrowType.Union) {
      return "union";
    }
    if (type instanceof ArrowType.Null) {
      return "null";
    }
    return type.getTypeID().name().toLowerCase(Locale.ROOT);
  }
}
