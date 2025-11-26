/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Principal;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.Privilege;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test of {@link PassThroughAuthorizer} */
public class TestPassThroughAuthorizer {

  @Test
  public void testPassThroughAuthorizerDefaults() throws IOException {
    try (PassThroughAuthorizer passThroughAuthorizer = new PassThroughAuthorizer()) {
      Principal principal = mock(Principal.class);
      when(principal.getName()).thenReturn("testUser");
      MetadataObject metadataObject =
          MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA);
      Assertions.assertTrue(
          passThroughAuthorizer.authorize(
              principal,
              "metalake",
              metadataObject,
              Privilege.Name.CREATE_SCHEMA,
              new AuthorizationRequestContext()));

      Assertions.assertFalse(
          passThroughAuthorizer.deny(
              principal,
              "metalake",
              metadataObject,
              Privilege.Name.CREATE_SCHEMA,
              new AuthorizationRequestContext()));

      Assertions.assertTrue(passThroughAuthorizer.isOwner(principal, "metalake", metadataObject));
      Assertions.assertTrue(passThroughAuthorizer.isServiceAdmin());
      Assertions.assertTrue(passThroughAuthorizer.isMetalakeUser("metalake"));
      Assertions.assertTrue(passThroughAuthorizer.isSelf(Entity.EntityType.USER, null));
      Assertions.assertTrue(
          passThroughAuthorizer.hasSetOwnerPermission(
              "metalake", "type", "fullName", new AuthorizationRequestContext()));
      Assertions.assertTrue(
          passThroughAuthorizer.hasMetadataPrivilegePermission(
              "metalake", "type", "fullName", new AuthorizationRequestContext()));
    }
  }
}
