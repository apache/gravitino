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
 * Represents an event that is generated when an attempt to get URI of a model version fails due to
 * an exception.
 */
@DeveloperApi
public class GetModelVersionUriFailureEvent extends ModelFailureEvent {
  private final Optional<String> alias;
  private final Optional<Integer> version;
  private final String uriName;

  /**
   * Construct a new {@link GetModelVersionUriFailureEvent} instance, capturing detailed information
   * about the failed attempt to get URI of a model version. only one of alias or version are valid.
   *
   * @param user The user associated with the failed model operation.
   * @param identifier The identifier of the model that was involved in the failed operation.
   * @param exception The exception that was thrown during the get model version operation, offering
   *     insights into what went wrong and why the operation failed.
   * @param alias The alias of the model version to get.
   * @param version The version of the model version to get.
   * @param uriName The URI name of the model version to get.
   */
  public GetModelVersionUriFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      String alias,
      Integer version,
      String uriName) {
    super(user, identifier, exception);

    this.alias = Optional.ofNullable(alias);
    this.version = Optional.ofNullable(version);
    this.uriName = uriName;
  }

  /**
   * Returns the alias of the model version to be accessed.
   *
   * @return A {@link Optional} instance containing the alias if it was provided, or an empty {@link
   *     Optional} otherwise.
   */
  public Optional<String> alias() {
    return alias;
  }

  /**
   * Returns the version of the model version to be accessed.
   *
   * @return A {@link Optional} instance containing the version if it was provided, or an empty
   *     {@link Optional} otherwise.
   */
  public Optional<Integer> version() {
    return version;
  }

  /**
   * Returns the URI name of the model version to be accessed.
   *
   * @return The name of the URI.
   */
  public String uriName() {
    return uriName;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_MODEL_VERSION_URI;
  }
}
