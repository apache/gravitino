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
package org.apache.gravitino.idp.authorization;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;

/** Utility methods for built-in IdP authorization identifiers and namespaces. */
public final class IdpAuthorizationUtils {

  static final String IDP_USER_DOES_NOT_EXIST_MSG = "IdP user %s does not exist";
  static final String IDP_GROUP_DOES_NOT_EXIST_MSG = "IdP group %s does not exist";

  /** The reserved namespace for built-in IdP entities. */
  public static final String IDP_RESERVED_NAMESPACE = "idp";

  private IdpAuthorizationUtils() {}

  /**
   * Returns the namespace for built-in IdP users.
   *
   * @return The namespace for built-in IdP users.
   */
  public static Namespace ofIdpUserNamespace() {
    return Namespace.of(IDP_RESERVED_NAMESPACE, Entity.USER_SCHEMA_NAME);
  }

  /**
   * Returns the namespace for built-in IdP groups.
   *
   * @return The namespace for built-in IdP groups.
   */
  public static Namespace ofIdpGroupNamespace() {
    return Namespace.of(IDP_RESERVED_NAMESPACE, Entity.GROUP_SCHEMA_NAME);
  }

  /**
   * Returns the name identifier for a built-in IdP user.
   *
   * @param username The username.
   * @return The name identifier for the built-in IdP user.
   */
  public static NameIdentifier ofIdpUser(String username) {
    return NameIdentifier.of(IDP_RESERVED_NAMESPACE, Entity.USER_SCHEMA_NAME, username);
  }

  /**
   * Returns the name identifier for a built-in IdP group.
   *
   * @param groupName The group name.
   * @return The name identifier for the built-in IdP group.
   */
  public static NameIdentifier ofIdpGroup(String groupName) {
    return NameIdentifier.of(IDP_RESERVED_NAMESPACE, Entity.GROUP_SCHEMA_NAME, groupName);
  }

  /**
   * Validates the namespace for a built-in IdP user.
   *
   * @param namespace The namespace to validate.
   */
  public static void checkIdpUserNamespace(Namespace namespace) {
    NameIdentifier.check(namespace != null, "IdP user namespace must not be null");
    NameIdentifier.check(
        namespace.equals(ofIdpUserNamespace()), "Invalid IdP user namespace: %s", namespace);
  }

  /**
   * Validates the namespace for a built-in IdP group.
   *
   * @param namespace The namespace to validate.
   */
  public static void checkIdpGroupNamespace(Namespace namespace) {
    NameIdentifier.check(namespace != null, "IdP group namespace must not be null");
    NameIdentifier.check(
        namespace.equals(ofIdpGroupNamespace()), "Invalid IdP group namespace: %s", namespace);
  }

  /**
   * Validates the name identifier for a built-in IdP user.
   *
   * @param ident The name identifier to validate.
   */
  public static void checkIdpUser(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "IdP user identifier must not be null");
    checkIdpUserNamespace(ident.namespace());
  }

  /**
   * Validates the name identifier for a built-in IdP group.
   *
   * @param ident The name identifier to validate.
   */
  public static void checkIdpGroup(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "IdP group identifier must not be null");
    checkIdpGroupNamespace(ident.namespace());
  }
}
