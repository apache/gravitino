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
package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

/** Tests Spark-visible Doris schema projections and identifier quoting. */
public class TestDorisReadSchema35 {

  @Test
  void testTableOrQueryQuotesDatabaseAndTable() {
    DorisReadSchema35 schema =
        new DorisReadSchema35(
            DataTypes.createStructType(
                new org.apache.spark.sql.types.StructField[] {
                  DataTypes.createStructField("column", DataTypes.StringType, true)
                }),
            Arrays.asList("`column`"),
            false,
            ImmutableSet.of());

    assertEquals(
        "(SELECT `column` FROM `db``name`.`table name`) gravitino_doris_source",
        schema.tableOrQuery(Identifier.of(new String[] {"db`name"}, "table name")));
  }

  @Test
  void testProjectionCountMustMatchSchema() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DorisReadSchema35(
                DataTypes.createStructType(new org.apache.spark.sql.types.StructField[0]),
                Arrays.asList("`unexpected`"),
                false,
                ImmutableSet.of()));
  }
}
