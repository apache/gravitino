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
package org.apache.gravitino.catalog.lakehouse.iceberg.converter;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Type converter belonging to Apache Gravitino.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/SparkTypeVisitor.java
 */
public class ToIcebergTypeVisitor<T> {

  /**
   * Traverse the Gravitino data type and convert the fields into Iceberg fields.
   *
   * @param type Gravitino a data type in a Gravitino.
   * @param visitor Visitor of Iceberg type
   * @param <T> Iceberg type
   * @return Iceberg type
   */
  public static <T> T visit(Type type, ToIcebergTypeVisitor<T> visitor) {
    if (type instanceof Types.MapType) {
      Types.MapType map = (Types.MapType) type;
      return visitor.map(map, visit(map.keyType(), visitor), visit(map.valueType(), visitor));
    } else if (type instanceof Types.ListType) {
      Types.ListType list = (Types.ListType) type;
      return visitor.array(list, visit(list.elementType(), visitor));
    } else if (type instanceof Types.StructType) {
      Types.StructType.Field[] fields = ((Types.StructType) type).fields();
      List<T> fieldResults = Lists.newArrayListWithExpectedSize(fields.length);
      for (Types.StructType.Field field : fields) {
        fieldResults.add(visitor.field(field, visit(field.type(), visitor)));
      }
      return visitor.struct((Types.StructType) type, fieldResults);
    } else {
      return visitor.atomic((Type.PrimitiveType) type);
    }
  }

  public T struct(IcebergTable struct, List<T> fieldResults) {
    throw new UnsupportedOperationException();
  }

  public T struct(Types.StructType struct, List<T> fieldResults) {
    throw new UnsupportedOperationException();
  }

  public T field(Types.StructType.Field field, T typeResult) {
    throw new UnsupportedOperationException();
  }

  public T array(Types.ListType array, T elementResult) {
    throw new UnsupportedOperationException();
  }

  public T map(Types.MapType map, T keyResult, T valueResult) {
    throw new UnsupportedOperationException();
  }

  public T atomic(Type.PrimitiveType primitive) {
    throw new UnsupportedOperationException();
  }
}
