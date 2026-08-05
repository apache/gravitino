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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/**
 * A group change is a change to a group. It can be used to update the optional external id.
 * Multiple changes may be applied in one {@code alterGroupById} call.
 */
@Evolving
public interface GroupChange {

  /**
   * Creates a group change to update the external identifier.
   *
   * @param newExternalId The new external identifier, or null to clear it.
   * @return The group change.
   */
  static GroupChange updateExternalId(@Nullable String newExternalId) {
    return new UpdateExternalId(newExternalId);
  }

  /** A group change to update the external identifier. */
  final class UpdateExternalId implements GroupChange {
    @Nullable private final String newExternalId;

    private UpdateExternalId(@Nullable String newExternalId) {
      this.newExternalId = newExternalId;
    }

    /**
     * Returns the new external identifier, or null to clear it.
     *
     * @return The new external identifier, or null.
     */
    @Nullable
    public String getNewExternalId() {
      return newExternalId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof UpdateExternalId)) {
        return false;
      }
      UpdateExternalId that = (UpdateExternalId) o;
      return Objects.equals(newExternalId, that.newExternalId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newExternalId);
    }

    @Override
    public String toString() {
      return "UpdateExternalId " + newExternalId;
    }
  }
}
