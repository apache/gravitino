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
import org.apache.gravitino.listener.api.info.ModelVersionInfo;

/**
 * Represents an event that is triggered when an attempt to link a model version fails due to an
 * exception.
 */
@DeveloperApi
public class LinkModelVersionFailureEvent extends ModelFailureEvent {
  private final ModelVersionInfo linkModelVersionRequest;
  /**
   * Construct a new {@link LinkModelVersionFailureEvent} instance, capturing information about the
   * failed model version operation.
   *
   * @param user The username of the individual who initiated the operation to link a model version.
   * @param identifier The identifier of the model that was involved in the failed operation.
   * @param exception The exception encountered during the attempt to link a model version.
   * @param linkModelVersionRequest The original {@link ModelVersionInfo} request containing details
   *     of the model version link operation that failed.
   */
  public LinkModelVersionFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      ModelVersionInfo linkModelVersionRequest) {
    super(user, identifier, exception);

    this.linkModelVersionRequest = linkModelVersionRequest;
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
    return OperationType.LINK_MODEL_VERSION;
  }
}
