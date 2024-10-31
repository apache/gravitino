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

import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.TestOperationDispatcher;
import org.apache.gravitino.catalog.TestTopicOperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestTopicHookDispatcher extends TestOperationDispatcher {
  private static TopicHookDispatcher topicHookDispatcher;
  private static SchemaHookDispatcher schemaHookDispatcher;
  private static AccessControlManager accessControlManager =
      Mockito.mock(AccessControlManager.class);
  private static AuthorizationPlugin authorizationPlugin;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    TestTopicOperationDispatcher.initialize();

    topicHookDispatcher =
        new TopicHookDispatcher(TestTopicOperationDispatcher.getTopicOperationDispatcher());
    schemaHookDispatcher =
        new SchemaHookDispatcher(TestTopicOperationDispatcher.getSchemaOperationDispatcher());

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
    Namespace topicNs = Namespace.of(metalake, catalog, "schema1123");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaHookDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    NameIdentifier topicIdent = NameIdentifier.of(topicNs, "topicNAME");
    topicHookDispatcher.createTopic(topicIdent, "comment", null, props);

    Mockito.reset(authorizationPlugin);
    topicHookDispatcher.dropTopic(topicIdent);
    Mockito.verify(authorizationPlugin).onMetadataUpdated(any());
  }
}
