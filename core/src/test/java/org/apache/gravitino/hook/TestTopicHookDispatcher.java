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
import static org.mockito.ArgumentMatchers.eq;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.TestOperationDispatcher;
import org.apache.gravitino.catalog.TestTopicOperationDispatcher;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.messaging.Topic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestTopicHookDispatcher extends TestOperationDispatcher {
  private static TopicHookDispatcher topicHookDispatcher;
  private static SchemaHookDispatcher schemaHookDispatcher;
  private static AccessControlManager accessControlManager =
      Mockito.mock(AccessControlManager.class);
  private static AuthorizationPlugin authorizationPlugin;

  @BeforeAll
  public static void initialize() throws Exception {
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
    Mockito.when(catalog.capability()).thenReturn(Capability.DEFAULT);
    CatalogManager.CatalogWrapper catalogWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(catalogWrapper.catalog()).thenReturn(catalog);
    Mockito.when(catalogWrapper.capabilities()).thenReturn(Capability.DEFAULT);
    Mockito.when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    Mockito.when(catalogManager.loadCatalogAndWrap(any())).thenReturn(catalogWrapper);
    authorizationPlugin = Mockito.mock(AuthorizationPlugin.class);
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);
  }

  @Test
  public void testCreateTopicSetsOwnerWithNormalizedIdentifier() throws Exception {
    // Self-contained: use a fresh hook with a directly-mocked TopicDispatcher and a case-
    // insensitive catalog so we can verify the helper passes a normalized ident to setOwner.
    CatalogManager savedCatalogManager = GravitinoEnv.getInstance().catalogManager();
    OwnerDispatcher savedOwnerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();

    CatalogManager mockCatalogManager = Mockito.mock(CatalogManager.class);
    CatalogManager.CatalogWrapper mockWrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(mockWrapper.capabilities()).thenReturn(new CaseInsensitiveCapability());
    Mockito.when(mockCatalogManager.loadCatalogAndWrap(any())).thenReturn(mockWrapper);

    OwnerDispatcher mockOwnerDispatcher = Mockito.mock(OwnerDispatcher.class);
    TopicDispatcher mockTopicDispatcher = Mockito.mock(TopicDispatcher.class);
    Mockito.when(mockTopicDispatcher.createTopic(any(), any(), any(), any()))
        .thenReturn(Mockito.mock(Topic.class));

    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", mockCatalogManager, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);

    try {
      TopicHookDispatcher localHook = new TopicHookDispatcher(mockTopicDispatcher);
      NameIdentifier ident = NameIdentifier.of(metalake, catalog, "SCHEMA_NORM", "MY_TOPIC");
      localHook.createTopic(ident, "comment", null, ImmutableMap.of());

      ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
      Mockito.verify(mockOwnerDispatcher)
          .setOwner(eq(metalake), captor.capture(), any(), eq(Owner.Type.USER));
      Assertions.assertEquals(
          "my_topic",
          captor.getValue().name(),
          "Topic name passed to setOwner must be lowercased by Capability.Scope.TOPIC normalization");
      Assertions.assertEquals(
          catalog + ".schema_norm",
          captor.getValue().parent(),
          "Topic parent (catalog.schema) must have its schema component lowercased by"
              + " Capability.Scope.TOPIC namespace normalization");
    } finally {
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "catalogManager", savedCatalogManager, true);
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "ownerDispatcher", savedOwnerDispatcher, true);
    }
  }

  @Test
  public void testCreateTopicSucceedsEvenIfSetOwnerFails() throws IllegalAccessException {
    OwnerDispatcher mockOwnerDispatcher = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);

    try {
      Namespace topicNs = Namespace.of(metalake, catalog, "schema_owner_fail");
      Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
      schemaHookDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

      NameIdentifier topicIdent = NameIdentifier.of(topicNs, "topic_owner_fail");
      topicHookDispatcher.createTopic(topicIdent, "comment", null, props);
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", null, true);
    }
  }

  @Test
  public void testDropAuthorizationPrivilege() {
    Namespace topicNs = Namespace.of(metalake, catalog, "schema1123");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaHookDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    NameIdentifier topicIdent = NameIdentifier.of(topicNs, "topicNAME");
    topicHookDispatcher.createTopic(topicIdent, "comment", null, props);

    withMockedAuthorizationUtils(
        () -> {
          topicHookDispatcher.dropTopic(topicIdent);
        });
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
