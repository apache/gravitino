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

package org.apache.gravitino.secret;

import java.util.Objects;

/** Immutable context for writing a secret at entity create time. */
public final class SecretWriteContext {

  private final String providerName;
  private final String entityType;
  private final long entityId;
  private final String propertyKey;

  /**
   * Creates a secret write context.
   *
   * @param providerName the configured secret provider name used in the URN
   * @param entityType the entity type (catalog, schema, or fileset)
   * @param entityId the entity identifier
   * @param propertyKey the property key holding the secret
   */
  public SecretWriteContext(
      String providerName, String entityType, long entityId, String propertyKey) {
    this.providerName = providerName;
    this.entityType = entityType;
    this.entityId = entityId;
    this.propertyKey = propertyKey;
  }

  /**
   * Returns the configured secret provider name.
   *
   * @return the provider name
   */
  public String providerName() {
    return providerName;
  }

  /**
   * Returns the entity type.
   *
   * @return the entity type
   */
  public String entityType() {
    return entityType;
  }

  /**
   * Returns the entity identifier.
   *
   * @return the entity identifier
   */
  public long entityId() {
    return entityId;
  }

  /**
   * Returns the property key.
   *
   * @return the property key
   */
  public String propertyKey() {
    return propertyKey;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SecretWriteContext)) {
      return false;
    }
    SecretWriteContext that = (SecretWriteContext) other;
    return entityId == that.entityId
        && Objects.equals(providerName, that.providerName)
        && Objects.equals(entityType, that.entityType)
        && Objects.equals(propertyKey, that.propertyKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(providerName, entityType, entityId, propertyKey);
  }
}
