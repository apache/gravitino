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
import org.apache.gravitino.listener.api.info.MetalakeInfo;

/** Represents an event triggered upon the successful creation of a Metalake. */
@DeveloperApi
public final class CreateMetalakeEvent extends MetalakeEvent {
  private final MetalakeInfo createdMetalakeInfo;
  /**
   * Constructs an instance of {@code CreateMetalakeEvent}, capturing essential details about the
   * successful creation of a metalake.
   *
   * @param user The username of the individual who initiated the metalake creation.
   * @param identifier The unique identifier of the metalake that was created.
   * @param createdMetalakeInfo The final state of the metalake post-creation.
   */
  public CreateMetalakeEvent(
      String user, NameIdentifier identifier, MetalakeInfo createdMetalakeInfo) {
    super(user, identifier);
    this.createdMetalakeInfo = createdMetalakeInfo;
  }

  /**
   * Retrieves the final state of the Metalake as it was returned to the user after successful
   * creation.
   *
   * @return A {@link MetalakeInfo} instance encapsulating the comprehensive details of the newly
   *     created Metalake.
   */
  public MetalakeInfo createdMetalakeInfo() {
    return createdMetalakeInfo;
  }
}
