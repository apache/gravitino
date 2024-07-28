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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event that occurs after a table is successfully purged from the database. */
@DeveloperApi
public final class PurgeTableEvent extends TableEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code PurgeTableEvent} instance.
   *
   * @param user The user who initiated the purge table operation.
   * @param identifier The identifier of the table that was targeted for purging.
   * @param isExists A boolean indicator reflecting whether the table was present in the database at
   *     the time of the purge operation.
   */
  public PurgeTableEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the status of the table's existence at the time of the purge operation.
   *
   * @return A boolean value indicating the table's existence status. {@code true} signifies that
   *     the table was present before the operation, {@code false} indicates it was not.
   */
  public boolean isExists() {
    return isExists;
  }
}
