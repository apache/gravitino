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
package org.apache.gravitino.authorization;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestAuthorizationUtils {

  String metalake = "metalake";

  @Test
  void testCreateNameIdentifier() {
    NameIdentifier user = AuthorizationUtils.ofUser(metalake, "user");
    NameIdentifier group = AuthorizationUtils.ofGroup(metalake, "group");
    NameIdentifier role = AuthorizationUtils.ofRole(metalake, "role");

    Assertions.assertEquals(AuthorizationUtils.ofUserNamespace(metalake), user.namespace());
    Assertions.assertEquals("user", user.name());
    Assertions.assertEquals(AuthorizationUtils.ofGroupNamespace(metalake), group.namespace());
    Assertions.assertEquals("group", group.name());
    Assertions.assertEquals(AuthorizationUtils.ofRoleNamespace(metalake), role.namespace());
    Assertions.assertEquals("role", role.name());
  }

  @Test
  void testCreateNameIdentifierWithInvalidArgs() {
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofUser(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofUser(metalake, ""));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofGroup(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofGroup(metalake, ""));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofRole(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofRole(metalake, ""));
  }

  @Test
  void testCreateNamespace() {
    Namespace namespace = AuthorizationUtils.ofUserNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("user", namespace.level(2));

    namespace = AuthorizationUtils.ofGroupNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("group", namespace.level(2));

    namespace = AuthorizationUtils.ofRoleNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("role", namespace.level(2));
  }

  @Test
  void testCreateNamespaceWithInvalidArgs() {
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofUserNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofUserNamespace(""));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofGroupNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofGroupNamespace(""));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofRoleNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofRoleNamespace(""));
  }

  @Test
  void testCheckNameIdentifier() {
    NameIdentifier user = AuthorizationUtils.ofUser(metalake, "user");
    NameIdentifier group = AuthorizationUtils.ofGroup(metalake, "group");
    NameIdentifier role = AuthorizationUtils.ofRole(metalake, "role");

    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkUser(user));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkGroup(group));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkRole(role));

    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkUser(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkGroup(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkRole(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkUser(NameIdentifier.of("")));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkGroup(NameIdentifier.of("")));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkRole(NameIdentifier.of("")));
  }

  @Test
  void testCheckNamespace() {
    Namespace userNamespace = AuthorizationUtils.ofUserNamespace(metalake);
    Namespace groupNamespace = AuthorizationUtils.ofGroupNamespace(metalake);
    Namespace roleNamespace = AuthorizationUtils.ofRoleNamespace(metalake);

    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkUserNamespace(userNamespace));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkGroupNamespace(groupNamespace));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkRoleNamespace(roleNamespace));

    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkUserNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkGroupNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkRoleNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkUserNamespace(Namespace.of("a", "b")));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkGroupNamespace(Namespace.of("a")));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkRoleNamespace(Namespace.of("a", "b", "c", "d")));
  }
}
