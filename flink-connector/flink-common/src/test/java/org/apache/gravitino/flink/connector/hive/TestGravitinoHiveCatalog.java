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
package org.apache.gravitino.flink.connector.hive;

import java.util.Collections;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGravitinoHiveCatalog {

  @Test
  public void testGetTableThrowsCatalogExceptionWhenForbidden() throws Exception {
    Catalog gravitinoCatalog = Mockito.mock(Catalog.class);
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    ForbiddenException forbiddenException = new ForbiddenException("denied");
    Mockito.when(gravitinoCatalog.asTableCatalog()).thenReturn(tableCatalog);
    Mockito.when(tableCatalog.loadTable(Mockito.any())).thenThrow(forbiddenException);
    TestableGravitinoHiveCatalog catalog = new TestableGravitinoHiveCatalog(gravitinoCatalog);

    CatalogException catalogException =
        Assertions.assertThrows(
            CatalogException.class, () -> catalog.getTable(new ObjectPath("db", "tbl")));

    Assertions.assertSame(forbiddenException, catalogException.getCause());
  }

  private static class TestableGravitinoHiveCatalog extends GravitinoHiveCatalog {
    private final Catalog gravitinoCatalog;

    TestableGravitinoHiveCatalog(Catalog gravitinoCatalog) {
      super(
          "test",
          "default",
          Collections.emptyMap(),
          Mockito.mock(SchemaAndTablePropertiesConverter.class),
          Mockito.mock(PartitionConverter.class),
          hiveConf(),
          null);
      this.gravitinoCatalog = gravitinoCatalog;
    }

    @Override
    protected AbstractCatalog realCatalog() {
      return Mockito.mock(AbstractCatalog.class);
    }

    @Override
    protected Catalog catalog() {
      return gravitinoCatalog;
    }

    private static HiveConf hiveConf() {
      HiveConf hiveConf = new HiveConf();
      hiveConf.set("hive.metastore.uris", "thrift://localhost:9083");
      return hiveConf;
    }
  }
}
