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

/**
 * Represents an event that is generated after a model version is successfully deleted from the
 * model.
 */
@DeveloperApi
public class DeleteModelVersionEvent extends ModelEvent {

  private final boolean isExists;
  private final Optional<String> alias;
  private final Optional<Integer> version;

  /**
   * Constructs a new {@code DeleteModelVersionEvent} instance, encapsulating information about the
   * outcome of a model version drop operation.
   *
   * @param user The user who initiated the drop model version operation.
   * @param identifier The identifier of the model that was attempted to be dropped a version.
   * @param isExists A boolean flag indicating whether the model version existed at the time of the
   *     drop operation.
   * @param alias The alias of the model version to be deleted. If the alias is not provided, the
   *     value will be an empty {@link Optional}.
   * @param version The version of the model version to be deleted. If the version is not provided,
   *     the value will be an empty {@link Optional}.
   */
  public DeleteModelVersionEvent(
      String user, NameIdentifier identifier, boolean isExists, String alias, Integer version) {
    super(user, identifier);

    this.isExists = isExists;
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
   * Retrieves the existence status of the model version at the time of the drop operation.
   *
   * @return A boolean value indicating whether the model version existed. {@code true} if the model
   *     version existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.DELETE_MODEL_VERSION;
  }
}
