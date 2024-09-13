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
package org.apache.gravitino.catalog.lakehouse.hudi.backend.hms;

import static org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata.URI;
import static org.apache.gravitino.catalog.lakehouse.hudi.HudiSchemaPropertiesMetadata.LOCATION;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiSchema;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiTable;
import org.apache.gravitino.hive.hms.MiniHiveMetastoreService;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestHudiHMSBackendOps extends MiniHiveMetastoreService {

  private static final HudiHMSBackendOps ops = new HudiHMSBackendOps();
  private static final String METALAKE_NAME = "metalake";
  private static final String CATALOG_NAME = "catalog";
  private static final String TABLE_NAME = "hudi_table";

  @BeforeAll
  public static void prepare() throws TException {
    Map<String, String> props = Maps.newHashMap();
    props.put(URI, hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
    ops.initialize(props);

    Table table = new Table();
    table.setDbName(DB_NAME);
    table.setTableName(TABLE_NAME);
    StorageDescriptor strgDesc = new StorageDescriptor();
    strgDesc.setCols(Lists.newArrayList(new FieldSchema("col1", "string", "description")));
    strgDesc.setSerdeInfo(new SerDeInfo());
    table.setSd(strgDesc);
    metastoreClient.createTable(table);
  }

  @AfterAll
  public static void cleanup() throws TException {
    ops.close();
  }

  @Test
  public void testInitialize() {
    try (HudiHMSBackendOps ops = new HudiHMSBackendOps()) {
      ops.initialize(ImmutableMap.of());
      Assertions.assertNotNull(ops.clientPool);
    }
  }

  @Test
  public void testLoadSchema() {
    HudiSchema hudiSchema = ops.loadSchema(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DB_NAME));

    Assertions.assertEquals(DB_NAME, hudiSchema.name());
    Assertions.assertEquals("description", hudiSchema.comment());
    Assertions.assertNotNull(hudiSchema.properties().get(LOCATION));
  }

  @Test
  public void testListSchemas() {
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME);
    NameIdentifier[] schemas = ops.listSchemas(namespace);

    Assertions.assertTrue(schemas.length > 0);
    Assertions.assertTrue(Arrays.stream(schemas).anyMatch(schema -> schema.name().equals(DB_NAME)));
  }

  @Test
  public void testListTables() {
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME, DB_NAME);
    NameIdentifier[] tables = ops.listTables(namespace);

    Assertions.assertTrue(Arrays.stream(tables).anyMatch(table -> table.name().equals(TABLE_NAME)));
  }

  @Test
  public void testLoadTable() {
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME, DB_NAME);
    HudiTable hudiTable = ops.loadTable(NameIdentifier.of(namespace, TABLE_NAME));

    Assertions.assertEquals(TABLE_NAME, hudiTable.name());
    Assertions.assertNull(hudiTable.comment());
    Assertions.assertNotNull(hudiTable.properties().get(LOCATION));

    Column[] columns = hudiTable.columns();
    Assertions.assertEquals(1, columns.length);

    Column column = columns[0];
    Assertions.assertEquals("col1", column.name());
    Assertions.assertEquals(Types.StringType.get(), column.dataType());
    Assertions.assertEquals("description", column.comment());
  }
}
