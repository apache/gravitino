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
import org.apache.gravitino.listener.api.info.ModelInfo;
import org.apache.gravitino.listener.api.info.ModelVersionInfo;

/**
 * Represents an event that is generated when an attempt to register a model or link a model version
 * of a model fails due to an exception.
 */
@DeveloperApi
public class RegisterAndLinkModelFailureEvent extends ModelFailureEvent {
  private final ModelInfo registerModelRequest;
  private final ModelVersionInfo linkModelVersionRequest;

  /**
   * Create a new {@link RegisterAndLinkModelFailureEvent} instance, capturing detailed information
   * about * the failed attempt to register a model.
   *
   * @param user The user who initiated the register/link model operation.
   * @param identifier The identifier of the model that was involved in the failed operation.
   * @param exception The exception encountered during the attempt to register a model or link a
   *     model version.
   * @param registerModelRequest the model information that was requested to be registered.
   * @param linkModelVersionRequest The version information of the model that was requested to be
   *     linked.
   */
  public RegisterAndLinkModelFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      ModelInfo registerModelRequest,
      ModelVersionInfo linkModelVersionRequest) {
    super(user, identifier, exception);

    this.registerModelRequest = registerModelRequest;
    this.linkModelVersionRequest = linkModelVersionRequest;
  }

  /**
   * Retrieves the registered model information.
   *
   * @return the model information.
   */
  public ModelInfo registerModelRequest() {
    return registerModelRequest;
  }

  /**
   * Retrieves the linked model version information.
   *
   * @return the model version information.
   */
  public ModelVersionInfo linkModelVersionRequest() {
    return linkModelVersionRequest;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REGISTER_AND_LINK_MODEL_VERSION;
  }
}
