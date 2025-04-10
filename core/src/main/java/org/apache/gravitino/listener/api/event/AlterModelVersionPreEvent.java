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
import org.apache.gravitino.model.ModelVersionChange;

/** Represents an event triggered before a model version is successfully altered. */
@DeveloperApi
public class AlterModelVersionPreEvent extends ModelPreEvent {
  private final Optional<String> alias;
  private final Optional<Integer> version;
  private ModelVersionChange[] modelVersionChanges;

  /**
   * Constructs a new {@code AlterModelVersionPreEvent} instance. Only one of {@code alias} or
   * {@code version} should be specified.
   *
   * @param user the user who triggered the event
   * @param identifier the identifier of the model involved in the event
   * @param alias the alias of the model version, or {@code null} if not specified
   * @param version the version of the model, or {@code null} if not specified
   * @param modelVersionChanges an array of {@code ModelVersionChange} instances representing the
   *     changes to apply
   */
  public AlterModelVersionPreEvent(
      String user,
      NameIdentifier identifier,
      String alias,
      Integer version,
      ModelVersionChange[] modelVersionChanges) {
    super(user, identifier);

    this.alias = Optional.ofNullable(alias);
    this.version = Optional.ofNullable(version);
    this.modelVersionChanges = modelVersionChanges;
  }

  /**
   * Returns the alias of the model version involved in the event.
   *
   * @return an {@code Optional} containing the alias if specified, otherwise an empty {@code
   *     Optional}
   */
  public Optional<String> alias() {
    return alias;
  }

  /**
   * Returns the version of the model involved in the event.
   *
   * @return an {@code Optional} containing the version if specified, otherwise an empty {@code
   *     Optional}
   */
  public Optional<Integer> version() {
    return version;
  }

  /**
   * Returns the model version changes to be applied.
   *
   * @return an array of {@code ModelVersionChange} instances
   */
  public ModelVersionChange[] modelVersionChanges() {
    return modelVersionChanges;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_MODEL_VERSION;
  }
}
