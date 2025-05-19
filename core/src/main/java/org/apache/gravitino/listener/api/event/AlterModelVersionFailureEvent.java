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

import com.google.common.base.Supplier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.Either;
import org.apache.gravitino.model.ModelVersionChange;

/** Represents an event triggered when a model version alteration operation fails. */
@DeveloperApi
public class AlterModelVersionFailureEvent extends ModelFailureEvent {
  private final Either<String, Integer> aliasOrVersion;
  private ModelVersionChange[] modelVersionChanges;

  /**
   * Constructs a new {@code AlterModelVersionFailureEvent} instance. Only one of {@code alias} or
   * {@code version} should be specified.
   *
   * @param user the user who triggered the event
   * @param identifier the identifier of the model involved in the event
   * @param exception the exception that caused the failure
   * @param aliasOrVersion the alias or version of the model version involved in the event
   * @param modelVersionChanges an array of {@code ModelVersionChange} instances that were attempted
   *     to be applied
   */
  public AlterModelVersionFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      Either<String, Integer> aliasOrVersion,
      ModelVersionChange[] modelVersionChanges) {
    super(user, identifier, exception);

    this.aliasOrVersion = aliasOrVersion;
    this.modelVersionChanges = modelVersionChanges;
  }

  /**
   * Returns the alias of the model version involved in the event.
   *
   * @return if the left value of {@code aliasOrVersion} is not null, returns the left value,
   *     otherwise throw IllegalStateException.
   */
  public String alias() {
    return aliasOrVersion
        .left()
        .orElseThrow(
            (Supplier<IllegalStateException>)
                () -> new IllegalStateException("Alias can't be null value"));
  }

  /**
   * Returns the version of the model version involved in the event.
   *
   * @return if the right value of {@code aliasOrVersion} is not null, returns the right value,
   *     otherwise throw IllegalStateException.
   */
  public Integer version() {
    return aliasOrVersion
        .right()
        .orElseThrow(
            (Supplier<IllegalStateException>)
                () -> new IllegalStateException("Version can't be null value"));
  }

  /**
   * Returns the model version changes that were attempted.
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
