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
package org.apache.gravitino.hook;

import static org.apache.gravitino.Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.SERVICE_ADMINS;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.TestFilesetOperationDispatcher;
import org.apache.gravitino.catalog.TestOperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.lock.LockManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestFilesetHookDispatcher extends TestOperationDispatcher {

  private static FilesetHookDispatcher filesetHookDispatcher;
  private static SchemaHookDispatcher schemaHookDispatcher;
  private static AccessControlManager accessControlManager =
      Mockito.mock(AccessControlManager.class);
  private static AuthorizationPlugin authorizationPlugin;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    TestFilesetOperationDispatcher.initialize();

    filesetHookDispatcher =
        new FilesetHookDispatcher(TestFilesetOperationDispatcher.getFilesetOperationDispatcher());
    schemaHookDispatcher =
        new SchemaHookDispatcher(TestFilesetOperationDispatcher.getSchemaOperationDispatcher());

    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlDispatcher", accessControlManager, true);
    catalogManager = Mockito.mock(CatalogManager.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", catalogManager, true);
    BaseCatalog catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    authorizationPlugin = Mockito.mock(AuthorizationPlugin.class);
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);
  }

  @Test
  public void testDropAuthorizationPrivilege() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema11212");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaHookDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent = NameIdentifier.of(filesetNs, "filesetNAME1");
    filesetHookDispatcher.createFileset(
        filesetIdent, "comment", Fileset.Type.MANAGED, "fileset41", props);

    withMockedAuthorizationUtils(
        () -> {
          filesetHookDispatcher.dropFileset(filesetIdent);
          Config config = Mockito.mock(Config.class);
          Mockito.when(config.get(SERVICE_ADMINS))
              .thenReturn(Lists.newArrayList("admin1", "admin2"));
          Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
          Mockito.when(config.get(ENTITY_RELATIONAL_STORE))
              .thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
          Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
              .thenReturn(
                  String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", "/tmp/testdb"));
          Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
              .thenReturn("org.h2.Driver");
          Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
          Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
              .thenReturn(1000L);
          Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
          Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
          Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
          Mockito.when(config.get(CATALOG_CACHE_EVICTION_INTERVAL_MS)).thenReturn(1000L);
          Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
          Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
          Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
          try {
            FieldUtils.writeField(
                GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
          schemaHookDispatcher.dropSchema(NameIdentifier.of(filesetNs.levels()), true);
        });
  }

  @Test
  public void testRenameAuthorizationPrivilege() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema1121");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaHookDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent = NameIdentifier.of(filesetNs, "filesetNAME2");
    filesetHookDispatcher.createFileset(
        filesetIdent, "comment", Fileset.Type.MANAGED, "fileset41", props);

    Mockito.reset(authorizationPlugin);
    FilesetChange setChange = FilesetChange.setProperty("k1", "v1");
    filesetHookDispatcher.alterFileset(filesetIdent, setChange);
    Mockito.verify(authorizationPlugin, Mockito.never()).onMetadataUpdated(any());

    Mockito.reset(authorizationPlugin);
    FilesetChange renameChange = FilesetChange.rename("newName");
    filesetHookDispatcher.alterFileset(filesetIdent, renameChange);
    Mockito.verify(authorizationPlugin).onMetadataUpdated(any());
  }
}
