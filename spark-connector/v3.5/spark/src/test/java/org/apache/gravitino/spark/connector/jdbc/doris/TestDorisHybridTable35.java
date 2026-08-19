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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.junit.jupiter.api.Test;

/** Tests the read-only capability contract of the Spark 3.5 Doris table facade. */
public class TestDorisHybridTable35 {

  @Test
  void testOnlyExposesBatchReadCapability() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.name()).thenReturn("table");
    when(logicalTable.properties()).thenReturn(Collections.emptyMap());
    PropertiesConverter propertiesConverter = mock(PropertiesConverter.class);
    when(propertiesConverter.toSparkTableProperties(anyMap())).thenReturn(Collections.emptyMap());

    org.apache.spark.sql.connector.catalog.Table nativeTable =
        mock(
            org.apache.spark.sql.connector.catalog.Table.class,
            org.mockito.Mockito.withSettings().extraInterfaces(SupportsRead.class));
    org.apache.spark.sql.connector.catalog.Table jdbcTable =
        mock(
            org.apache.spark.sql.connector.catalog.Table.class,
            org.mockito.Mockito.withSettings().extraInterfaces(SupportsRead.class));
    DorisReadSchema35 readSchema =
        new DorisReadSchema35(
            org.apache.spark.sql.types.DataTypes.createStructType(
                new org.apache.spark.sql.types.StructField[0]),
            Collections.emptyList(),
            false,
            Collections.emptySet());

    DorisHybridTable35 table =
        new DorisHybridTable35(
            Identifier.of(new String[] {"db"}, "table"),
            logicalTable,
            nativeTable,
            jdbcTable,
            readSchema,
            propertiesConverter,
            mock(SparkTransformConverter.class),
            mock(SparkTypeConverter.class));

    assertEquals(ImmutableSet.of(TableCapability.BATCH_READ), table.capabilities());
  }
}
