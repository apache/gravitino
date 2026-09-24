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
package org.apache.gravitino.storage;

import java.util.Objects;

/**
 * The identity and store version of an entity registration, as read at one moment.
 *
 * <p>A dispatcher reads this before it calls the external catalog and hands it back to the store
 * afterwards, so a delete only touches the registration the operation started with. The id fences
 * the external call; the store checks the current version when committing the delete. Without it, a
 * store write that resolves the name again after the external call can land on an entity that was
 * re-created under the same name in between.
 */
public final class EntityVersion {

  private final long id;
  private final long version;

  private EntityVersion(long id, long version) {
    this.id = id;
    this.version = version;
  }

  /**
   * Creates a version token.
   *
   * @param id the entity id
   * @param version the store version of the entity row
   * @return the token
   */
  public static EntityVersion of(long id, long version) {
    return new EntityVersion(id, version);
  }

  /**
   * Returns the entity id.
   *
   * @return the entity id
   */
  public long id() {
    return id;
  }

  /**
   * Returns the store version of the entity row.
   *
   * @return the version
   */
  public long version() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EntityVersion)) {
      return false;
    }
    EntityVersion that = (EntityVersion) o;
    return id == that.id && version == that.version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, version);
  }

  @Override
  public String toString() {
    return "EntityVersion{id=" + id + ", version=" + version + "}";
  }
}
