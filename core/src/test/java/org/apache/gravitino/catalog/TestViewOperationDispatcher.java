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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.View;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestViewOperationDispatcher extends TestOperationDispatcher {
  static ViewOperationDispatcher viewOperationDispatcher;
  static SchemaOperationDispatcher schemaOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    viewOperationDispatcher = new ViewOperationDispatcher(catalogManager, entityStore, idGenerator);

    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "schemaDispatcher", schemaOperationDispatcher, true);
  }

  public static ViewOperationDispatcher getViewOperationDispatcher() {
    return viewOperationDispatcher;
  }

  public static SchemaOperationDispatcher getSchemaOperationDispatcher() {
    return schemaOperationDispatcher;
  }

  public static CatalogManager getCatalogManager() {
    return catalogManager;
  }

  @Test
  public void testLoadView() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema61");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    NameIdentifier viewIdent1 = NameIdentifier.of(viewNs, "view1");

    // Create a mock view through the catalog operations
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    View mockView =
        new View() {
          @Override
          public String name() {
            return "view1";
          }

          @Override
          public Map<String, String> properties() {
            return props;
          }

          @Override
          public AuditInfo auditInfo() {
            return auditInfo;
          }
        };

    // Mock the catalog operations to return the view
    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();
    testCatalogOperations.views.put(viewIdent1, mockView);

    // Test load view
    View loadedView = viewOperationDispatcher.loadView(viewIdent1);
    Assertions.assertEquals("view1", loadedView.name());
    Assertions.assertEquals("test", loadedView.auditInfo().creator());

    // Test load non-existent view
    NameIdentifier viewIdent2 = NameIdentifier.of(viewNs, "non_existent_view");
    Assertions.assertThrows(
        NoSuchViewException.class, () -> viewOperationDispatcher.loadView(viewIdent2));
  }

  @Test
  public void testLoadViewWithInvalidNamespace() {
    Namespace invalidNs = Namespace.of(metalake, catalog, "non_existent_schema");
    NameIdentifier viewIdent = NameIdentifier.of(invalidNs, "view1");

    Assertions.assertThrows(
        NoSuchViewException.class, () -> viewOperationDispatcher.loadView(viewIdent));
  }

  @Test
  public void testLoadViewWithMultipleViews() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema62");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    // Create multiple views
    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();

    for (int i = 1; i <= 3; i++) {
      NameIdentifier viewIdent = NameIdentifier.of(viewNs, "view" + i);
      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
      int index = i;
      View mockView =
          new View() {
            @Override
            public String name() {
              return "view" + index;
            }

            @Override
            public Map<String, String> properties() {
              return props;
            }

            @Override
            public AuditInfo auditInfo() {
              return auditInfo;
            }
          };
      testCatalogOperations.views.put(viewIdent, mockView);
    }

    // Test loading each view
    for (int i = 1; i <= 3; i++) {
      NameIdentifier viewIdent = NameIdentifier.of(viewNs, "view" + i);
      View loadedView = viewOperationDispatcher.loadView(viewIdent);
      Assertions.assertEquals("view" + i, loadedView.name());
    }
  }
}
