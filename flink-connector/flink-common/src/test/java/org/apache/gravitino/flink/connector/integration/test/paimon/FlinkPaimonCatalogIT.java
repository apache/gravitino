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
package org.apache.gravitino.flink.connector.integration.test.paimon;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class FlinkPaimonCatalogIT extends FlinkCommonIT {

  protected org.apache.gravitino.Catalog catalog;

  @Override
  protected boolean supportSchemaOperationWithCommentAndOptions() {
    return false;
  }

  @Override
  protected String getProvider() {
    return "lakehouse-paimon";
  }

  @Override
  protected boolean supportDropCascade() {
    return true;
  }

  protected Catalog currentCatalog() {
    return catalog;
  }

  private void initPaimonCatalog() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    catalog =
        metalake.createCatalog(
            getPaimonCatalogName(),
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            getProvider(),
            null,
            getPaimonCatalogOptions());
  }

  protected abstract void createGravitinoCatalogByFlinkSql(String catalogName);

  protected abstract String getPaimonCatalogName();

  protected abstract Map<String, String> getPaimonCatalogOptions();

  @BeforeAll
  void paimonSetup() {
    initPaimonCatalog();
  }

  @AfterAll
  void paimonStop() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    metalake.dropCatalog(getPaimonCatalogName(), true);
  }

  protected abstract String getWarehouse();

  @Test
  public void testCreateGravitinoPaimonCatalogUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "gravitino_paimon_catalog";
    createGravitinoCatalogByFlinkSql(catalogName);
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(getWarehouse(), properties.get("warehouse"));
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs, tableEnv.listCatalogs().length, "The created catalog should be dropped.");
  }
}
