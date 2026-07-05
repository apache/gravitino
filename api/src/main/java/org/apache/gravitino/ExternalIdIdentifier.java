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

import java.util.Arrays;

/**
 * An identifier for entities located by external id within a metalake namespace.
 *
 * <p>This is distinct from {@link NameIdentifier}, which identifies entities by Gravitino name.
 */
public class ExternalIdIdentifier {

  private static final String SYSTEM_CATALOG_RESERVED_NAME = "system";
  private static final String USER_SCHEMA_NAME = "user";

  private final Namespace namespace;
  private final String externalId;

  /**
   * Creates an {@link ExternalIdIdentifier} for a user in the given metalake.
   *
   * @param metalake the metalake name
   * @param externalId the external id of the user
   * @return the external id identifier
   */
  public static ExternalIdIdentifier ofUser(String metalake, String externalId) {
    return new ExternalIdIdentifier(
        Namespace.of(metalake, SYSTEM_CATALOG_RESERVED_NAME, USER_SCHEMA_NAME), externalId);
  }

  private ExternalIdIdentifier(Namespace namespace, String externalId) {
    NameIdentifier.check(
        externalId != null && !externalId.isEmpty(), "External id must not be null or empty");
    this.namespace = namespace;
    this.externalId = externalId;
  }

  /**
   * Returns the namespace of this identifier.
   *
   * @return the namespace
   */
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the external id value.
   *
   * @return the external id
   */
  public String externalId() {
    return externalId;
  }

  /**
   * Returns the metalake name.
   *
   * @return the metalake name
   */
  public String metalake() {
    return namespace.level(0);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ExternalIdIdentifier)) {
      return false;
    }
    ExternalIdIdentifier that = (ExternalIdIdentifier) other;
    return namespace.equals(that.namespace) && externalId.equals(that.externalId);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new int[] {namespace.hashCode(), externalId.hashCode()});
  }

  @Override
  public String toString() {
    return namespace + "." + externalId;
  }
}
