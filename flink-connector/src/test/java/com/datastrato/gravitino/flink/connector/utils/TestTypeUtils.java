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

package com.datastrato.gravitino.flink.connector.utils;

import com.datastrato.gravitino.rel.types.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTypeUtils {

  @Test
  public void testToGravitinoType() {
    Assertions.assertEquals(
        Types.StringType.get(), TypeUtils.toGravitinoType(new VarCharType(Integer.MAX_VALUE)));
    Assertions.assertEquals(Types.DoubleType.get(), TypeUtils.toGravitinoType(new DoubleType()));
    Assertions.assertEquals(Types.IntegerType.get(), TypeUtils.toGravitinoType(new IntType()));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            TypeUtils.toGravitinoType(
                new UnresolvedUserDefinedType(UnresolvedIdentifier.of("a", "b", "c"))));
  }

  @Test
  public void testToFlinkType() {
    Assertions.assertEquals(DataTypes.DOUBLE(), TypeUtils.toFlinkType(Types.DoubleType.get()));
    Assertions.assertEquals(DataTypes.STRING(), TypeUtils.toFlinkType(Types.StringType.get()));
    Assertions.assertEquals(DataTypes.INT(), TypeUtils.toFlinkType(Types.IntegerType.get()));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> TypeUtils.toFlinkType(Types.UnparsedType.of("unknown")));
  }
}
