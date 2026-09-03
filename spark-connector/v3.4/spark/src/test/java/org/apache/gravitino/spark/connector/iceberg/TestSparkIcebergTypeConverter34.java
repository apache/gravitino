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
import org.apache.gravitino.spark.connector.SparkTableChangeConverter34;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types.UnknownType;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSparkIcebergTypeConverter34 {

  private final SparkIcebergTypeConverter34 converter = new SparkIcebergTypeConverter34();

  @Test
  void testUnknownTypeConversion() {
    Assertions.assertEquals(DataTypes.NullType, converter.toSparkType(NullType.get()));
    Assertions.assertEquals(NullType.get(), converter.toGravitinoType(DataTypes.NullType));
  }

  @Test
  void testPinnedIcebergUnknownSchemaRoundTrip() {
    StructType sparkSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("payload", DataTypes.NullType, true)
            });

    Schema icebergSchema = SparkSchemaUtil.convert(sparkSchema);
    Assertions.assertInstanceOf(UnknownType.class, icebergSchema.findType("payload"));
    Assertions.assertTrue(icebergSchema.findField("payload").isOptional());

    StructType roundTripped = SparkSchemaUtil.convert(icebergSchema);
    Assertions.assertEquals(DataTypes.NullType, roundTripped.apply("payload").dataType());
    Assertions.assertTrue(roundTripped.apply("payload").nullable());
  }

  @Test
  void testRejectRequiredUnknownBeforeAlter() {
    SparkTableChangeConverter34 changeConverter = new SparkTableChangeConverter34(converter);
    TableChange change = TableChange.addColumn(new String[] {"payload"}, DataTypes.NullType, false);

    IllegalArgumentException exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> changeConverter.toGravitinoTableChange(change));
    Assertions.assertTrue(exception.getMessage().contains("must be nullable"));
  }
}
