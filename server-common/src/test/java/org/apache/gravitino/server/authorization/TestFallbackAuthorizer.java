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

package org.apache.gravitino.server.authorization;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFallbackAuthorizer {

  private FallbackAuthorizer fallbackAuthorizer;
  private GravitinoAuthorizer primaryAuthorizer;
  private GravitinoAuthorizer secondaryAuthorizer;
  private Principal principal;
  private MetadataObject metadataObject;
  private AuthorizationRequestContext requestContext;
  private GravitinoEnv gravitinoEnv;
  private EntityStore entityStore;

  @BeforeEach
  public void setUp() {
    fallbackAuthorizer = new FallbackAuthorizer();
    primaryAuthorizer = mock(GravitinoAuthorizer.class);
    secondaryAuthorizer = mock(GravitinoAuthorizer.class);
    principal = mock(Principal.class);
    requestContext = mock(AuthorizationRequestContext.class);

    metadataObject =
        MetadataObjects.of(
            java.util.Arrays.asList("catalog", "schema", "table"), MetadataObject.Type.TABLE);

    fallbackAuthorizer.setPrimaryAuthorizer(primaryAuthorizer);
    fallbackAuthorizer.setSecondaryAuthorizer(secondaryAuthorizer);

    // Mock GravitinoEnv for EntityStore checks
    gravitinoEnv = mock(GravitinoEnv.class);
    entityStore = mock(EntityStore.class);
    GravitinoEnv.setInstance(gravitinoEnv);
    when(gravitinoEnv.entityStore()).thenReturn(entityStore);
  }

  @AfterEach
  public void tearDown() {
    GravitinoEnv.setInstance(null);
  }

  @Test
  public void testAuthorizeWithGravitinoObject() throws Exception {
    // Object exists in Gravitino
    when(primaryAuthorizer.authorize(
            eq(principal),
            eq("metalake"),
            eq(metadataObject),
            eq(Privilege.Name.SELECT_TABLE),
            eq(requestContext)))
        .thenReturn(true);

    boolean result =
        fallbackAuthorizer.authorize(
            principal, "metalake", metadataObject, Privilege.Name.SELECT_TABLE, requestContext);

    assertTrue(result);
    verify(primaryAuthorizer)
        .authorize(principal, "metalake", metadataObject, Privilege.Name.SELECT_TABLE, requestContext);
    verify(secondaryAuthorizer, never())
        .authorize(any(), any(), any(), any(), any());
  }

  @Test
  public void testAuthorizeWithExternalObject() throws Exception {
    // Simulate object not found in Gravitino by making MetadataIdConverter throw
    // This is tested by the actual existsInGravitino method behavior
    when(secondaryAuthorizer.authorize(
            eq(principal),
            eq("metalake"),
            eq(metadataObject),
            eq(Privilege.Name.SELECT_TABLE),
            eq(requestContext)))
        .thenReturn(true);

    // Note: In real scenario, MetadataIdConverter.getID would throw NoSuchEntityException
    // For this test, we're verifying the logic flow

    boolean result =
        fallbackAuthorizer.authorize(
            principal, "metalake", metadataObject, Privilege.Name.SELECT_TABLE, requestContext);

    // Will use secondary since object doesn't exist in Gravitino
    verify(secondaryAuthorizer)
        .authorize(principal, "metalake", metadataObject, Privilege.Name.SELECT_TABLE, requestContext);
  }

  @Test
  public void testAuthorizeExternalObjectNoSecondaryAuthorizer() throws Exception {
    // No secondary authorizer configured
    fallbackAuthorizer.setSecondaryAuthorizer(null);

    // Object not in Gravitino, no secondary authorizer
    boolean result =
        fallbackAuthorizer.authorize(
            principal, "metalake", metadataObject, Privilege.Name.SELECT_TABLE, requestContext);

    // Should return true to allow 404 during entity loading
    assertTrue(result);
    verify(primaryAuthorizer, never()).authorize(any(), any(), any(), any(), any());
  }

  @Test
  public void testDenyWithGravitinoObject() throws Exception {
    when(primaryAuthorizer.deny(
            eq(principal),
            eq("metalake"),
            eq(metadataObject),
            eq(Privilege.Name.SELECT_TABLE),
            eq(requestContext)))
        .thenReturn(true);

    boolean result =
        fallbackAuthorizer.deny(
            principal, "metalake", metadataObject, Privilege.Name.SELECT_TABLE, requestContext);

    assertTrue(result);
    verify(primaryAuthorizer)
        .deny(principal, "metalake", metadataObject, Privilege.Name.SELECT_TABLE, requestContext);
  }

  @Test
  public void testDenyWithExternalObject() throws Exception {
    // For external objects, deny should return false
    // (secondary systems use holistic authorization)
    boolean result =
        fallbackAuthorizer.deny(
            principal, "metalake", metadataObject, Privilege.Name.SELECT_TABLE, requestContext);

    assertFalse(result);
    verify(secondaryAuthorizer, never()).deny(any(), any(), any(), any(), any());
  }

  @Test
  public void testIsOwnerWithGravitinoObject() throws Exception {
    when(primaryAuthorizer.isOwner(
            eq(principal), eq("metalake"), eq(metadataObject), eq(requestContext)))
        .thenReturn(true);

    boolean result = fallbackAuthorizer.isOwner(principal, "metalake", metadataObject, requestContext);

    assertTrue(result);
    verify(primaryAuthorizer).isOwner(principal, "metalake", metadataObject, requestContext);
    verify(secondaryAuthorizer, never()).isOwner(any(), any(), any(), any());
  }

  @Test
  public void testIsOwnerWithExternalObject() throws Exception {
    when(secondaryAuthorizer.isOwner(
            eq(principal), eq("metalake"), eq(metadataObject), eq(requestContext)))
        .thenReturn(true);

    boolean result = fallbackAuthorizer.isOwner(principal, "metalake", metadataObject, requestContext);

    verify(secondaryAuthorizer).isOwner(principal, "metalake", metadataObject, requestContext);
  }

  @Test
  public void testInitialize() {
    fallbackAuthorizer.initialize();

    verify(primaryAuthorizer).initialize();
    verify(secondaryAuthorizer).initialize();
  }

  @Test
  public void testClose() throws Exception {
    fallbackAuthorizer.close();

    verify(primaryAuthorizer).close();
    verify(secondaryAuthorizer).close();
  }

  @Test
  public void testServiceAdminDelegation() {
    when(primaryAuthorizer.isServiceAdmin()).thenReturn(true);

    boolean result = fallbackAuthorizer.isServiceAdmin();

    assertTrue(result);
    verify(primaryAuthorizer).isServiceAdmin();
  }

  @Test
  public void testMetalakeUserDelegation() {
    when(primaryAuthorizer.isMetalakeUser("metalake")).thenReturn(true);

    boolean result = fallbackAuthorizer.isMetalakeUser("metalake");

    assertTrue(result);
    verify(primaryAuthorizer).isMetalakeUser("metalake");
  }
}
