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

import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.MetalakeInfo;

/** Represents an event fired when a metalake is successfully altered. */
@DeveloperApi
public final class AlterMetalakeEvent extends MetalakeEvent {
  private final MetalakeInfo updatedMetalakeInfo;
  private final MetalakeChange[] metalakeChanges;

  /**
   * Constructs an instance of {@code AlterMetalakeEvent}, encapsulating the key details about the
   * successful alteration of a metalake.
   *
   * @param user The username of the individual responsible for initiating the metalake alteration.
   * @param identifier The unique identifier of the altered metalake, serving as a clear reference
   *     point for the metalake in question.
   * @param metalakeChanges An array of {@link MetalakeChange} objects representing the specific
   *     changes applied to the metalake during the alteration process.
   * @param updatedMetalakeInfo The post-alteration state of the metalake.
   */
  public AlterMetalakeEvent(
      String user,
      NameIdentifier identifier,
      MetalakeChange[] metalakeChanges,
      MetalakeInfo updatedMetalakeInfo) {
    super(user, identifier);
    this.metalakeChanges = metalakeChanges.clone();
    this.updatedMetalakeInfo = updatedMetalakeInfo;
  }

  /**
   * Retrieves the updated state of the metalake after the successful alteration.
   *
   * @return A {@link MetalakeInfo} instance encapsulating the details of the altered metalake.
   */
  public MetalakeInfo updatedMetalakeInfo() {
    return updatedMetalakeInfo;
  }

  /**
   * Retrieves the specific changes that were made to the metalake during the alteration process.
   *
   * @return An array of {@link MetalakeChange} objects detailing each modification applied to the
   *     metalake.
   */
  public MetalakeChange[] metalakeChanges() {
    return metalakeChanges;
  }
}
