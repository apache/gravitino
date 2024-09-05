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

/** Represents an event that is generated when a Metalake is successfully loaded. */
@DeveloperApi
public final class LoadMetalakeEvent extends MetalakeEvent {
  private final MetalakeInfo loadedMetalakeInfo;

  /**
   * Constructs an instance of {@code LoadMetalakeEvent}.
   *
   * @param user The username of the individual who initiated the metalake loading.
   * @param identifier The unique identifier of the metalake that was loaded.
   * @param metalakeInfo The state of the metalake post-loading.
   */
  public LoadMetalakeEvent(String user, NameIdentifier identifier, MetalakeInfo metalakeInfo) {
    super(user, identifier);
    this.loadedMetalakeInfo = metalakeInfo;
  }

  /**
   * Retrieves detailed information about the Metalake that was successfully loaded.
   *
   * @return A {@link MetalakeInfo} instance containing comprehensive details of the Metalake,
   *     including its configuration, properties, and state at the time of loading.
   */
  public MetalakeInfo loadedMetalakeInfo() {
    return loadedMetalakeInfo;
  }
}
