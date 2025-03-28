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

/** Represents an event that is generated after a model version listing operation. */
public class ListModelVersionsEvent extends ModelEvent {
  private final int[] versions;

  /**
   * Constructs an instance of {@code ListModelVersionsEvent}, with the specified user and model
   * identifier.
   *
   * @param user The username of the individual who initiated the model version listing.
   * @param identifier The unique identifier of the model that it's version was listed.
   */
  public ListModelVersionsEvent(String user, NameIdentifier identifier, int[] versions) {
    super(user, identifier);

    this.versions = versions;
  }

  /**
   * Returns the versions of the model in this list operation.
   *
   * @return The versions of the model.
   */
  public int[] versions() {
    return versions;
  }

  /**
   * Returns the type of operation.
   *
   * @return The operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_MODEL_VERSIONS;
  }
}
