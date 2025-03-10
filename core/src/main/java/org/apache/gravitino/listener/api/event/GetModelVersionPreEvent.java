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

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event triggered before getting the version of a model. */
@DeveloperApi
public class GetModelVersionPreEvent extends ModelPreEvent {
  private final Optional<String> alias;
  private final Optional<Integer> version;

  /**
   * Create a new {@link GetModelVersionPreEvent} instance with optional alias and version
   * arguments. only one of alias or version are valid.
   *
   * @param user The username of the individual who initiated the model version to get.
   * @param identifier The unique identifier of the model that was getting the version.
   * @param alias The alias of the model version to get.
   * @param version The version of the model version to get.
   */
  public GetModelVersionPreEvent(
      String user, NameIdentifier identifier, String alias, Integer version) {
    super(user, identifier);

    this.alias = Optional.ofNullable(alias);
    this.version = Optional.ofNullable(version);
  }

  /**
   * Returns the alias of the model version to be deleted.
   *
   * @return A {@link Optional} instance containing the alias if it was provided, or an empty {@link
   *     Optional} otherwise.
   */
  public Optional<String> alias() {
    return alias;
  }

  /**
   * Returns the version of the model version to be deleted.
   *
   * @return A {@link Optional} instance containing the version if it was provided, or an empty
   *     {@link Optional} otherwise.
   */
  public Optional<Integer> version() {
    return version;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_MODEL_VERSION;
  }
}
