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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestCatalogObjectExistence {

  private static final NameIdentifier IDENT = NameIdentifier.of("ml", "cat", "s", "object");

  private BaseCatalog<?> catalog;
  private CatalogManager manager;
  private FilesetOperationDispatcher filesets;
  private ModelOperationDispatcher models;
  private TopicOperationDispatcher topics;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    manager = mock(CatalogManager.class);
    catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(new Capability() {});
    CatalogTestUtils.mockDoWithCatalog(manager, catalog);
    CatalogManager.CatalogWrapper wrapper =
        CatalogTestUtils.mockDetachConnectorResult(mock(CatalogManager.CatalogWrapper.class));
    doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, Object> operation = invocation.getArgument(0);
              return operation.apply(catalog);
            })
        .when(wrapper)
        .doWithCatalog(any());
    doAnswer(
            invocation -> {
              ThrowableFunction<CatalogManager.CatalogWrapper, Object> operation =
                  invocation.getArgument(1);
              return operation.apply(wrapper);
            })
        .when(manager)
        .doWithCatalogWrapper(any(), any());
    EntityStore store = mock(EntityStore.class);
    IdGenerator ids = mock(IdGenerator.class);
    SecretManager secrets = mock(SecretManager.class);
    filesets = spy(new FilesetOperationDispatcher(manager, store, ids, secrets));
    models = spy(new ModelOperationDispatcher(manager, store, ids, secrets));
    topics = spy(new TopicOperationDispatcher(manager, store, ids, secrets));
  }

  @Test
  void testUnsupportedCatalogObjectsReturnNotFound() {
    when(catalog.ops()).thenReturn(mock(CatalogOperations.class));
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.internalFilesetDispatcher())
        .thenReturn(new FilesetNormalizeDispatcher(filesets, manager));
    when(env.internalModelDispatcher()).thenReturn(new ModelNormalizeDispatcher(models, manager));
    when(env.internalTopicDispatcher()).thenReturn(new TopicNormalizeDispatcher(topics, manager));
    try (MockedStatic<GravitinoEnv> mocked = mockStatic(GravitinoEnv.class)) {
      mocked.when(GravitinoEnv::getInstance).thenReturn(env);
      for (MetadataObject.Type type :
          new MetadataObject.Type[] {
            MetadataObject.Type.FILESET, MetadataObject.Type.MODEL, MetadataObject.Type.TOPIC
          }) {
        Assertions.assertThrows(
            NoSuchMetadataObjectException.class,
            () ->
                MetadataObjectUtil.checkMetadataObject(
                    "ml", MetadataObjects.parse("cat.s.object", type)));
      }
    }
    verify(filesets, never()).loadFileset(any());
    verify(models, never()).getModel(any());
    verify(topics, never()).loadTopic(any());
  }

  @Test
  void testSupportedFilesetCatalogPreservesLoadBehavior() {
    when(catalog.ops()).thenReturn(mock(FilesetCatalogOperations.class));
    doReturn(mock(Fileset.class)).when(filesets).loadFileset(IDENT);
    Assertions.assertTrue(filesets.filesetExists(IDENT));
    verify(filesets).loadFileset(IDENT);
    doThrow(new NoSuchFilesetException("missing")).when(filesets).loadFileset(IDENT);
    Assertions.assertFalse(filesets.filesetExists(IDENT));
    doThrow(new UnsupportedOperationException("backend failure")).when(filesets).loadFileset(IDENT);
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> filesets.filesetExists(IDENT));
  }

  @Test
  void testSupportedModelCatalogPreservesLoadBehavior() {
    when(catalog.ops()).thenReturn(mock(ModelCatalogOperations.class));
    doReturn(mock(Model.class)).when(models).getModel(IDENT);
    Assertions.assertTrue(models.modelExists(IDENT));
    verify(models).getModel(IDENT);
    doThrow(new NoSuchModelException("missing")).when(models).getModel(IDENT);
    Assertions.assertFalse(models.modelExists(IDENT));
    doThrow(new UnsupportedOperationException("backend failure")).when(models).getModel(IDENT);
    Assertions.assertThrows(UnsupportedOperationException.class, () -> models.modelExists(IDENT));
  }

  @Test
  void testSupportedTopicCatalogPreservesLoadBehavior() {
    when(catalog.ops()).thenReturn(mock(TopicCatalogOperations.class));
    doReturn(mock(Topic.class)).when(topics).loadTopic(IDENT);
    Assertions.assertTrue(topics.topicExists(IDENT));
    verify(topics).loadTopic(IDENT);
    doThrow(new NoSuchTopicException("missing")).when(topics).loadTopic(IDENT);
    Assertions.assertFalse(topics.topicExists(IDENT));
    doThrow(new UnsupportedOperationException("backend failure")).when(topics).loadTopic(IDENT);
    Assertions.assertThrows(UnsupportedOperationException.class, () -> topics.topicExists(IDENT));
  }

  private interface FilesetCatalogOperations extends CatalogOperations, FilesetCatalog {}

  private interface ModelCatalogOperations extends CatalogOperations, ModelCatalog {}

  private interface TopicCatalogOperations extends CatalogOperations, TopicCatalog {}
}
