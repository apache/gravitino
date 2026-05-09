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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.gravitino.authorization.IdpManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestIdpManagerFactory {

  @Test
  public void testCreateIdpManagerLoadsPluginImplementation() {
    IdpManager idpManager = IdpManagerFactory.createIdpManager();
    assertEquals(
        "org.apache.gravitino.idp.basic.authorization.IdpManager", idpManager.getClass().getName());
  }

  @Test
  public void testCreateIdpManagerOrDefaultReturnsUnavailableManagerWhenNoProviderFound() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpManager> serviceLoader = mock(ServiceLoader.class);
      mockedLoader.when(() -> ServiceLoader.load(IdpManager.class)).thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(Collections.emptyIterator());

      IdpManager idpManager = IdpManagerFactory.createIdpManagerOrDefault();

      UnsupportedOperationException exception =
          assertThrows(UnsupportedOperationException.class, () -> idpManager.getUser("user1"));
      assertEquals(
          "Built-in IdP management is unavailable because no IdpManager plugin implementation"
              + " was found on the runtime classpath.",
          exception.getMessage());
    }
  }

  @Test
  public void testCreateFailsWhenMultipleProvidersFound() {
    try (MockedStatic<ServiceLoader> mockedLoader = Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<IdpManager> serviceLoader = mock(ServiceLoader.class);
      IdpManager first = mock(IdpManager.class);
      IdpManager second = mock(IdpManager.class);
      mockedLoader.when(() -> ServiceLoader.load(IdpManager.class)).thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(List.of(first, second).iterator());

      IllegalStateException exception =
          assertThrows(IllegalStateException.class, IdpManagerFactory::createIdpManager);
      assertEquals("Multiple IdpManager implementations found", exception.getMessage());
    }
  }
}
