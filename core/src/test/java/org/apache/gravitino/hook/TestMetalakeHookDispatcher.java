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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMetalakeHookDispatcher {

  private MetalakeHookDispatcher hookDispatcher;
  private MetalakeDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  private AccessControlDispatcher mockAccessControlDispatcher;
  // Save the originals before each test and restore them in tearDown so we do not leak null
  // state into the GravitinoEnv singleton across tests.
  private OwnerDispatcher savedOwnerDispatcher;
  private AccessControlDispatcher savedAccessControlDispatcher;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    mockDispatcher = mock(MetalakeDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    mockAccessControlDispatcher = mock(AccessControlDispatcher.class);
    savedOwnerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    savedAccessControlDispatcher = GravitinoEnv.getInstance().accessControlDispatcher();
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlDispatcher", mockAccessControlDispatcher, true);
    hookDispatcher = new MetalakeHookDispatcher(mockDispatcher);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "ownerDispatcher", savedOwnerDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlDispatcher", savedAccessControlDispatcher, true);
  }

  @Test
  public void testCreateMetalakeThrowsWhenSetOwnerFails() {
    NameIdentifier ident = NameIdentifier.of("test_metalake");
    Metalake mockMetalake = mock(Metalake.class);
    when(mockDispatcher.createMetalake(any(), any(), any())).thenReturn(mockMetalake);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> hookDispatcher.createMetalake(ident, "comment", Collections.emptyMap()));
    Assertions.assertEquals("Set owner failed", thrown.getMessage());
    verify(mockDispatcher).createMetalake(any(), any(), any());
  }

  @Test
  public void testCreateMetalakeThrowsWhenUserAlreadyExists() {
    NameIdentifier ident = NameIdentifier.of("test_metalake");
    Metalake mockMetalake = mock(Metalake.class);
    when(mockDispatcher.createMetalake(any(), any(), any())).thenReturn(mockMetalake);

    // With the addUser try-catch removed, UserAlreadyExistsException now propagates to the caller.
    // The caller can treat "already exists" as idempotent if they want; the server no longer
    // silently swallows it.
    doThrow(new UserAlreadyExistsException("User already exists"))
        .when(mockAccessControlDispatcher)
        .addUser(any(), any());

    UserAlreadyExistsException thrown =
        Assertions.assertThrows(
            UserAlreadyExistsException.class,
            () -> hookDispatcher.createMetalake(ident, "comment", Collections.emptyMap()));
    Assertions.assertEquals("User already exists", thrown.getMessage());
    verify(mockAccessControlDispatcher).addUser(any(), any());
    verify(mockOwnerDispatcher, never()).setOwner(any(), any(), any(), any());
  }

  @Test
  public void testCreateMetalakeThrowsWhenAddUserFails() {
    NameIdentifier ident = NameIdentifier.of("test_metalake");
    Metalake mockMetalake = mock(Metalake.class);
    when(mockDispatcher.createMetalake(any(), any(), any())).thenReturn(mockMetalake);

    // addUser failure (e.g. storage I/O) now propagates to the caller; setOwner is unreachable.
    doThrow(new RuntimeException("Add user failed"))
        .when(mockAccessControlDispatcher)
        .addUser(any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> hookDispatcher.createMetalake(ident, "comment", Collections.emptyMap()));
    Assertions.assertEquals("Add user failed", thrown.getMessage());
    verify(mockAccessControlDispatcher).addUser(any(), any());
    verify(mockOwnerDispatcher, never()).setOwner(any(), any(), any(), any());
  }
}
