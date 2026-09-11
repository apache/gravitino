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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

/** Tests the governed capability contract of the Spark 3.5 Doris table facade. */
@SuppressWarnings("deprecation")
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
            DataTypes.createStructType(new StructField[0]),
            Collections.emptyList(),
            false,
            Collections.emptyMap());

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
    assertThrows(
        UnsupportedOperationException.class,
        () -> table.newWriteBuilder(mock(LogicalWriteInfo.class)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            table.newScanBuilder(
                new CaseInsensitiveStringMap(
                    ImmutableMap.of("url", "jdbc:mysql://unmanaged.example:9030"))));
    verifyNoInteractions(nativeTable, jdbcTable);
  }

  @Test
  void testGovernedWriteValidatesSchemaBeforeDelegation() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.name()).thenReturn("table");
    when(logicalTable.properties()).thenReturn(Collections.emptyMap());
    when(logicalTable.columns())
        .thenReturn(
            new Column[] {
              Column.of(
                  "id", Types.IntegerType.get(), null, false, false, Column.DEFAULT_VALUE_NOT_SET)
            });
    PropertiesConverter propertiesConverter = mock(PropertiesConverter.class);
    when(propertiesConverter.toSparkTableProperties(anyMap())).thenReturn(Collections.emptyMap());
    org.apache.spark.sql.connector.catalog.Table nativeTable =
        mock(
            org.apache.spark.sql.connector.catalog.Table.class,
            withSettings().extraInterfaces(SupportsRead.class, SupportsWrite.class));
    org.apache.spark.sql.connector.catalog.Table jdbcTable =
        mock(
            org.apache.spark.sql.connector.catalog.Table.class,
            withSettings().extraInterfaces(SupportsRead.class));
    StructType schema = new StructType().add("id", DataTypes.IntegerType, false);
    DorisReadSchema35 readSchema =
        new DorisReadSchema35(
            schema, Collections.singletonList("`id`"), false, Collections.emptyMap());
    DorisHybridTable35 readTable =
        new DorisHybridTable35(
            Identifier.of(new String[] {"db"}, "table"),
            logicalTable,
            nativeTable,
            jdbcTable,
            readSchema,
            propertiesConverter,
            mock(SparkTransformConverter.class),
            mock(SparkTypeConverter.class));
    DorisWritePolicy35 writePolicy =
        DorisWritePolicy35.from(
            ImmutableMap.of(
                DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
                DorisConnectorConstants35.WRITE_BATCH));
    DorisHybridTable35 writable = readTable.withGovernedWrite(writePolicy);
    LogicalWriteInfo writeInfo = mock(LogicalWriteInfo.class);
    WriteBuilder delegate = mock(WriteBuilder.class);
    BatchWrite batch = mock(BatchWrite.class);
    when(writeInfo.options()).thenReturn(new CaseInsensitiveStringMap(ImmutableMap.of()));
    when(writeInfo.schema()).thenReturn(schema);
    when(((SupportsWrite) nativeTable).newWriteBuilder(writeInfo)).thenReturn(delegate);
    when(delegate.buildForBatch()).thenReturn(batch);

    assertEquals(
        ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE),
        writable.capabilities());
    WriteBuilder governed = writable.newWriteBuilder(writeInfo);
    assertNotSame(batch, governed.buildForBatch());
    verify((SupportsWrite) nativeTable).newWriteBuilder(writeInfo);

    LogicalWriteInfo incompatible = mock(LogicalWriteInfo.class);
    when(incompatible.options()).thenReturn(new CaseInsensitiveStringMap(ImmutableMap.of()));
    when(incompatible.schema()).thenReturn(new StructType().add("other", DataTypes.IntegerType));
    assertThrows(IllegalArgumentException.class, () -> writable.newWriteBuilder(incompatible));
    verify((SupportsWrite) nativeTable, never()).newWriteBuilder(incompatible);
  }
}
