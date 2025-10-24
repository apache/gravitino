/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.lakehouse.lance;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.gravitino.connector.DataTypeConverter;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rel.types.Types.FixedType;
import org.apache.gravitino.rel.types.Types.UnparsedType;

public class LanceDataTypeConverter implements DataTypeConverter<ArrowType, ArrowType> {

  @Override
  public ArrowType fromGravitino(Type type) {
    switch (type.name()) {
      case BOOLEAN:
        return Bool.INSTANCE;
      case BYTE:
        return new Int(8, true);
      case SHORT:
        return new Int(16, true);
      case INTEGER:
        return new Int(32, true);
      case LONG:
        return new Int(64, true);
      case FLOAT:
        return new FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE:
        return new FloatingPoint(FloatingPointPrecision.DOUBLE);
      case DECIMAL:
        // Lance uses FIXED_SIZE_BINARY for decimal types
        return new ArrowType.FixedSizeBinary(16); // assuming 16 bytes for decimal
      case DATE:
        return new ArrowType.Date(DateUnit.DAY);
      case TIME:
        return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case TIMESTAMP:
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
      case VARCHAR:
      case STRING:
        return new ArrowType.Utf8();
      case FIXED:
        FixedType fixedType = (FixedType) type;
        return new ArrowType.FixedSizeBinary(fixedType.length());
      case BINARY:
        return new ArrowType.Binary();
      case UNPARSED:
        String typeStr = ((UnparsedType) type).unparsedType().toString();
        try {
          Type t = JsonUtils.anyFieldMapper().readValue(typeStr, Type.class);
          if (t instanceof Types.ListType) {
            return ArrowType.List.INSTANCE;
          } else if (t instanceof Types.MapType) {
            return new ArrowType.Map(false);
          } else if (t instanceof Types.StructType) {
            return ArrowType.Struct.INSTANCE;
          } else {
            throw new UnsupportedOperationException(
                "Unsupported UnparsedType conversion: " + t.simpleString());
          }
        } catch (Exception e) {
          // FixedSizeListArray(integer, 3)
          if (typeStr.startsWith("FixedSizeListArray")) {
            int size =
                Integer.parseInt(
                    typeStr.substring(typeStr.indexOf(',') + 1, typeStr.indexOf(')')).trim());
            return new ArrowType.FixedSizeList(size);
          }
          throw new UnsupportedOperationException("Failed to parse UnparsedType: " + typeStr, e);
        }
      default:
        throw new UnsupportedOperationException("Unsupported Gravitino type: " + type.name());
    }
  }

  @Override
  public Type toGravitino(ArrowType arrowType) {
    if (arrowType instanceof Bool) {
      return Types.BooleanType.get();
    } else if (arrowType instanceof Int intType) {
      switch (intType.getBitWidth()) {
        case 8 -> {
          return Types.ByteType.get();
        }
        case 16 -> {
          return Types.ShortType.get();
        }
        case 32 -> {
          return Types.IntegerType.get();
        }
        case 64 -> {
          return Types.LongType.get();
        }
        default -> throw new UnsupportedOperationException(
            "Unsupported Int bit width: " + intType.getBitWidth());
      }
    } else if (arrowType instanceof FloatingPoint floatingPoint) {
      switch (floatingPoint.getPrecision()) {
        case SINGLE:
          return Types.FloatType.get();
        case DOUBLE:
          return Types.DoubleType.get();
        default:
          throw new UnsupportedOperationException(
              "Unsupported FloatingPoint precision: " + floatingPoint.getPrecision());
      }
    } else if (arrowType instanceof ArrowType.FixedSizeBinary) {
      ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) arrowType;
      return Types.FixedType.of(fixedSizeBinary.getByteWidth());
    } else if (arrowType instanceof ArrowType.Date) {
      return Types.DateType.get();
    } else if (arrowType instanceof ArrowType.Time) {
      return Types.TimeType.get();
    } else if (arrowType instanceof ArrowType.Timestamp) {
      return Types.TimestampType.withoutTimeZone();
    } else if (arrowType instanceof ArrowType.Utf8) {
      return Types.StringType.get();
    } else if (arrowType instanceof ArrowType.Binary) {
      return Types.BinaryType.get();
      // TODO handle complex types like List, Map, Struct
    } else {
      throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
    }
  }

  public List<ArrowType> getChildTypes(Type parentType) {
    if (parentType.name() != Type.Name.UNPARSED) {
      return List.of();
    }

    List<ArrowType> arrowTypes = Lists.newArrayList();
    String typeStr = ((UnparsedType) parentType).unparsedType().toString();
    try {
      Type t = JsonUtils.anyFieldMapper().readValue(typeStr, Type.class);
      if (t instanceof Types.ListType listType) {
        arrowTypes.add(fromGravitino(listType.elementType()));
      } else if (t instanceof Types.MapType mapType) {
        arrowTypes.add(fromGravitino(mapType.keyType()));
        arrowTypes.add(fromGravitino(mapType.valueType()));
      } else {
        // TODO support struct type.
        throw new UnsupportedOperationException(
            "Unsupported UnparsedType conversion: " + t.simpleString());
      }

      return arrowTypes;
    } catch (Exception e) {
      // FixedSizeListArray(integer, 3)

      try {
        if (typeStr.startsWith("FixedSizeListArray")) {
          String type = typeStr.substring(typeStr.indexOf('(') + 1, typeStr.indexOf(',')).trim();
          Type childType = JsonUtils.anyFieldMapper().readValue("\"" + type + "\"", Type.class);
          arrowTypes.add(fromGravitino(childType));

          return arrowTypes;
        }
      } catch (Exception e1) {
        throw new UnsupportedOperationException("Failed to parse UnparsedType: " + typeStr, e1);
      }

      throw new UnsupportedOperationException("Failed to parse UnparsedType: " + typeStr, e);
    }
  }
}
