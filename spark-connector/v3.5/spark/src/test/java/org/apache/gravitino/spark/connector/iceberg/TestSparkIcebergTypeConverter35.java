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
package org.apache.gravitino.spark.connector.iceberg;

import org.apache.gravitino.rel.types.Types.NullType;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types.UnknownType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSparkIcebergTypeConverter35 {

  @Test
  void testPinnedIcebergUnknownTypeRoundTrip() {
    SparkIcebergTypeConverter34 converter = new SparkIcebergTypeConverter34();
    Assertions.assertEquals(DataTypes.NullType, converter.toSparkType(NullType.get()));
    Assertions.assertEquals(NullType.get(), converter.toGravitinoType(DataTypes.NullType));

    Assertions.assertInstanceOf(UnknownType.class, SparkSchemaUtil.convert(DataTypes.NullType));
    Assertions.assertEquals(DataTypes.NullType, SparkSchemaUtil.convert(UnknownType.get()));
  }
}
