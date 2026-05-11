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
import org.apache.gravitino.storage.relational.po.IdpUserMeta;
import org.apache.gravitino.storage.relational.provider.IdpUserMetaProvider;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestIdpUserMetaService {
  @Test
  public void testFindUserDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      IdpUserMetaProvider provider = mock(IdpUserMetaProvider.class);
      IdpUserMeta userMeta = mock(IdpUserMeta.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.findUser("user")).thenReturn(Optional.of(userMeta));

      assertEquals(Optional.of(userMeta), IdpUserMetaService.getInstance().findUser("user"));
    }
  }

  @Test
  public void testFindUsersDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      IdpUserMetaProvider provider = mock(IdpUserMetaProvider.class);
      IdpUserMeta firstUser = mock(IdpUserMeta.class);
      IdpUserMeta secondUser = mock(IdpUserMeta.class);
      List<String> userNames = List.of("user1", "user2");
      List<IdpUserMeta> users = List.of(firstUser, secondUser);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.findUsers(userNames)).thenReturn(users);

      assertEquals(users, IdpUserMetaService.getInstance().findUsers(userNames));
    }
  }

  @Test
  public void testListGroupNamesDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      IdpUserMetaProvider provider = mock(IdpUserMetaProvider.class);
      List<String> groupNames = List.of("group1", "group2");
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.listGroupNames("user")).thenReturn(groupNames);

      assertEquals(groupNames, IdpUserMetaService.getInstance().listGroupNames("user"));
    }
  }

  @Test
  public void testCreateUserDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      IdpUserMetaProvider provider = mock(IdpUserMetaProvider.class);
      IdpUserMeta userMeta = mock(IdpUserMeta.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());

      IdpUserMetaService.getInstance().createUser(userMeta);

      verify(provider).createUser(userMeta);
    }
  }

  @Test
  public void testUpdatePasswordDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      IdpUserMetaProvider provider = mock(IdpUserMetaProvider.class);
      IdpUserMeta userMeta = mock(IdpUserMeta.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());

      IdpUserMetaService.getInstance().updatePassword(userMeta, "hash", 2L);

      verify(provider).updatePassword(userMeta, "hash", 2L);
    }
  }

  @Test
  public void testDeleteUserDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      IdpUserMetaProvider provider = mock(IdpUserMetaProvider.class);
      IdpUserMeta userMeta = mock(IdpUserMeta.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.deleteUser(userMeta, 3L)).thenReturn(true);

      assertEquals(true, IdpUserMetaService.getInstance().deleteUser(userMeta, 3L));
    }
  }

  @Test
  public void testDeleteUserMetasByLegacyTimelineDelegatesToPluginService() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      IdpUserMetaProvider provider = mock(IdpUserMetaProvider.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());
      when(provider.deleteUserMetasByLegacyTimeline(1L, 10)).thenReturn(3);

      assertEquals(3, IdpUserMetaService.getInstance().deleteUserMetasByLegacyTimeline(1L, 10));
    }
  }

  @Test
  public void testDeleteUserMetasByLegacyTimelineFailsWhenNoProviderFound() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(Collections.emptyIterator());

      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () -> IdpUserMetaService.getInstance().deleteUserMetasByLegacyTimeline(1L, 10));
      assertEquals("No IdpUserMetaProvider implementation found", exception.getMessage());
    }
  }

  @Test
  public void testDeleteUserMetasByLegacyTimelineFailsWhenMultipleProvidersFound() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpUserMetaProvider> serviceLoader = mock(ServiceLoader.class);
      IdpUserMetaProvider first = mock(IdpUserMetaProvider.class);
      IdpUserMetaProvider second = mock(IdpUserMetaProvider.class);
      mockedLoader
          .when(() -> ServiceLoader.load(IdpUserMetaProvider.class))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(first, second).iterator());

      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () -> IdpUserMetaService.getInstance().deleteUserMetasByLegacyTimeline(1L, 10));
      assertEquals("Multiple IdpUserMetaProvider implementations found", exception.getMessage());
    }
  }
}
