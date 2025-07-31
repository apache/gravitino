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

/** Represents an event triggered after the URI of a model version is successfully retrieved. */
@DeveloperApi
public class GetModelVersionUriEvent extends ModelEvent {
  private final Optional<String> alias;
  private final Optional<Integer> version;
  private final String uriName;
  private final String uri;

  /**
   * Create a new {@link GetModelVersionUriEvent} instance, containing the details of the event.
   * Only one of alias or version are valid.
   *
   * @param user The username of the individual who initiated the model version URI to get.
   * @param identifier The unique identifier of the model that was getting the version URI.
   * @param alias The alias of the model version to get.
   * @param version The version of the model version to get.
   * @param uriName The URI name of the model version to get.
   * @param uri The URI of the model version.
   */
  public GetModelVersionUriEvent(
      String user,
      NameIdentifier identifier,
      String alias,
      Integer version,
      String uriName,
      String uri) {
    super(user, identifier);

    this.alias = Optional.ofNullable(alias);
    this.version = Optional.ofNullable(version);
    this.uriName = uriName;
    this.uri = uri;
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
   * Returns the version number of the model version to be accessed.
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
   * Returns the URI of the model version to be accessed.
   *
   * @return The URI of the model version.
   */
  public String uri() {
    return uri;
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
