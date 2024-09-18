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
package org.apache.gravitino.hive.converter;

import static org.apache.gravitino.hive.converter.HiveDataTypeConverter.CONVERTER;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getDecimalTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getUnionTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

import java.util.Arrays;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTypeConverter {

  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testTypeConverter() {
    testConverter(BOOLEAN_TYPE_NAME);
    testConverter(TINYINT_TYPE_NAME);
    testConverter(SMALLINT_TYPE_NAME);
    testConverter(INT_TYPE_NAME);
    testConverter(BIGINT_TYPE_NAME);
    testConverter(FLOAT_TYPE_NAME);
    testConverter(DOUBLE_TYPE_NAME);
    testConverter(STRING_TYPE_NAME);
    testConverter(DATE_TYPE_NAME);
    testConverter(TIMESTAMP_TYPE_NAME);
    testConverter(BINARY_TYPE_NAME);
    testConverter(INTERVAL_YEAR_MONTH_TYPE_NAME);
    testConverter(INTERVAL_DAY_TIME_TYPE_NAME);
    testConverter(getVarcharTypeInfo(10).getTypeName());
    testConverter(getCharTypeInfo(20).getTypeName());
    testConverter(getDecimalTypeInfo(10, 2).getTypeName());

    testConverter(getListTypeInfo(getPrimitiveTypeInfo(STRING_TYPE_NAME)).getTypeName());
    testConverter(
        getMapTypeInfo(getPrimitiveTypeInfo(INT_TYPE_NAME), getPrimitiveTypeInfo(DATE_TYPE_NAME))
            .getTypeName());
    testConverter(
        getStructTypeInfo(
                Arrays.asList("a", "b"),
                Arrays.asList(
                    getPrimitiveTypeInfo(STRING_TYPE_NAME), getPrimitiveTypeInfo(INT_TYPE_NAME)))
            .getTypeName());
    testConverter(
        getUnionTypeInfo(
                Arrays.asList(
                    getPrimitiveTypeInfo(STRING_TYPE_NAME), getPrimitiveTypeInfo(INT_TYPE_NAME)))
            .getTypeName());
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> CONVERTER.fromGravitino(Types.ExternalType.of(USER_DEFINED_TYPE)));
  }

  private void testConverter(String typeName) {
    TypeInfo hiveType = getTypeInfoFromTypeString(typeName);
    TypeInfo convertedType = CONVERTER.fromGravitino(CONVERTER.toGravitino(hiveType.getTypeName()));
    Assertions.assertEquals(hiveType, convertedType);
  }
}
