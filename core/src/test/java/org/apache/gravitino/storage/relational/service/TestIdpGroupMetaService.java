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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestIdpGroupMetaService {
  @Test
  public void testFindGroupDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService provider = mock(IdpGroupMetaService.class);
      Object groupMeta = new Object();
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.findGroup("group")).thenReturn(Optional.of(groupMeta));

      assertEquals(Optional.of(groupMeta), IdpGroupMetaService.getInstance().findGroup("group"));
    }
  }

  @Test
  public void testListUserNamesDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService provider = mock(IdpGroupMetaService.class);
      List<String> userNames = List.of("user1", "user2");
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.listUserNames("group")).thenReturn(userNames);

      assertEquals(userNames, IdpGroupMetaService.getInstance().listUserNames("group"));
    }
  }

  @Test
  public void testCreateGroupDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService provider = mock(IdpGroupMetaService.class);
      Object groupMeta = new Object();
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());

      IdpGroupMetaService.getInstance().createGroup(groupMeta);

      verify(provider).createGroup(groupMeta);
    }
  }

  @Test
  public void testDeleteGroupDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService provider = mock(IdpGroupMetaService.class);
      Object groupMeta = new Object();
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.deleteGroup(groupMeta, 4L)).thenReturn(true);

      assertEquals(true, IdpGroupMetaService.getInstance().deleteGroup(groupMeta, 4L));
    }
  }

  @Test
  public void testSelectRelatedUserIdsDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService provider = mock(IdpGroupMetaService.class);
      List<Long> userIds = List.of(1L, 2L);
      List<Long> relatedUserIds = List.of(2L);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.selectRelatedUserIds(10L, userIds)).thenReturn(relatedUserIds);

      assertEquals(
          relatedUserIds, IdpGroupMetaService.getInstance().selectRelatedUserIds(10L, userIds));
    }
  }

  @Test
  public void testAddUsersToGroupDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService provider = mock(IdpGroupMetaService.class);
      Object relation = new Object();
      List<Object> relations = List.of(relation);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());

      IdpGroupMetaService.getInstance().addUsersToGroup(relations);

      verify(provider).addUsersToGroup(relations);
    }
  }

  @Test
  public void testRemoveUsersFromGroupDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService provider = mock(IdpGroupMetaService.class);
      List<Long> userIds = List.of(1L, 2L);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());

      IdpGroupMetaService.getInstance().removeUsersFromGroup(10L, userIds, 5L);

      verify(provider).removeUsersFromGroup(10L, userIds, 5L);
    }
  }

  @Test
  public void testDeleteGroupMetasByLegacyTimelineDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService provider = mock(IdpGroupMetaService.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.deleteGroupMetasByLegacyTimeline(2L, 10)).thenReturn(5);

      assertEquals(5, IdpGroupMetaService.getInstance().deleteGroupMetasByLegacyTimeline(2L, 10));
    }
  }

  @Test
  public void testDeleteGroupMetasByLegacyTimelineFailsWhenNoProviderFound() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(Collections.emptyIterator());

      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () -> IdpGroupMetaService.getInstance().deleteGroupMetasByLegacyTimeline(2L, 10));
      assertEquals("No IdpGroupMetaService implementation found", exception.getMessage());
    }
  }

  @Test
  public void testDeleteGroupMetasByLegacyTimelineFailsWhenMultipleProvidersFound() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpGroupMetaService> serviceLoader = mock(ServiceLoader.class);
      IdpGroupMetaService first = mock(IdpGroupMetaService.class);
      IdpGroupMetaService second = mock(IdpGroupMetaService.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpGroupMetaService.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(first, second).iterator());

      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () -> IdpGroupMetaService.getInstance().deleteGroupMetasByLegacyTimeline(2L, 10));
      assertEquals("Multiple IdpGroupMetaService implementations found", exception.getMessage());
    }
  }
}
