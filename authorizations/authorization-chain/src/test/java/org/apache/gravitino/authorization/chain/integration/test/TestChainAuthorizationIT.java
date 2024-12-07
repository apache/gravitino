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
package org.apache.gravitino.authorization.chain.integration.test;

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.getRangerAdminUrlKey;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.getRangerAuthTypeKey;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.getRangerPasswordKey;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.getRangerServiceNameKey;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.getRangerUsernameKey;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.authorization.ranger.integration.test.RangerHiveE2EIT;
import org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestChainAuthorizationIT extends RangerHiveE2EIT {
  private static final Logger LOG = LoggerFactory.getLogger(TestChainAuthorizationIT.class);
  private static final String CHAIN_PLUGIN_SHORT_NAME = "chain";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
  }

  @Override
  public void createCatalog() {
    String pluginName = "hive1";
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(HiveConstants.METASTORE_URIS, HIVE_METASTORE_URIS);
    catalogConf.put(IMPERSONATION_ENABLE, "true");
    catalogConf.put(AUTHORIZATION_PROVIDER, CHAIN_PLUGIN_SHORT_NAME);
    catalogConf.put(
        AuthorizationPropertiesMeta.getInstance().wildcardNodePropertyKey(), pluginName);
    catalogConf.put(
        AuthorizationPropertiesMeta.getInstance()
            .getPropertyValue(pluginName, AuthorizationPropertiesMeta.getChainProviderKey()),
        CHAIN_PLUGIN_SHORT_NAME);
    catalogConf.put(
        AuthorizationPropertiesMeta.getInstance()
            .getPropertyValue(pluginName, getRangerAuthTypeKey()),
        RangerContainer.authType);
    catalogConf.put(
        AuthorizationPropertiesMeta.getInstance()
            .getPropertyValue(pluginName, getRangerAdminUrlKey()),
        RANGER_ADMIN_URL);
    catalogConf.put(
        AuthorizationPropertiesMeta.getInstance()
            .getPropertyValue(pluginName, getRangerUsernameKey()),
        RangerContainer.rangerUserName);
    catalogConf.put(
        AuthorizationPropertiesMeta.getInstance()
            .getPropertyValue(pluginName, getRangerPasswordKey()),
        RangerContainer.rangerPassword);
    catalogConf.put(
        AuthorizationPropertiesMeta.getInstance()
            .getPropertyValue(pluginName, getRangerServiceNameKey()),
        RangerITEnv.RANGER_HIVE_REPO_NAME);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, "hive", "comment", catalogConf);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);
  }

  @Test
  public void testChainAuthorization() {
    LOG.info("");
  }

  @Override
  protected void checkTableAllPrivilegesExceptForCreating() {}

  @Override
  protected void checkUpdateSQLWithReadWritePrivileges() {}

  @Override
  protected void checkUpdateSQLWithReadPrivileges() {}

  @Override
  protected void checkUpdateSQLWithWritePrivileges() {}

  @Override
  protected void checkDeleteSQLWithReadWritePrivileges() {}

  @Override
  protected void checkDeleteSQLWithReadPrivileges() {}

  @Override
  protected void checkDeleteSQLWithWritePrivileges() {}

  @Override
  protected void useCatalog() throws InterruptedException {}

  @Override
  protected void checkWithoutPrivileges() {}

  @Override
  protected void testAlterTable() {}
}
