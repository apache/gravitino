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

/**
 * Represents an event that is generated when an attempt to create a Metalake fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateMetalakeFailureEvent extends MetalakeFailureEvent {
  private final MetalakeInfo createMetalakeRequest;

  public CreateMetalakeFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      MetalakeInfo createMetalakeRequest) {
    super(user, identifier, exception);
    this.createMetalakeRequest = createMetalakeRequest;
  }

  /**
   * Retrieves the original request information for the attempted Metalake creation.
   *
   * @return The {@link MetalakeInfo} instance representing the request information for the failed
   *     Metalake creation attempt.
   */
  public MetalakeInfo createMetalakeRequest() {
    return createMetalakeRequest;
  }
}
