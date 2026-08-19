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

package org.apache.gravitino.listener.api.info;

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.BasicUser;

/** Provides read-only access to basic user information for event listeners. */
@DeveloperApi
public class BasicUserInfo {
  private final Long id;
  private final String name;
  private final Optional<String> externalId;
  private final boolean enabled;

  /**
   * Construct a new {@link BasicUserInfo} instance with the given {@link BasicUser} information.
   *
   * @param user the {@link BasicUser} instance.
   */
  public BasicUserInfo(BasicUser user) {
    this.id = Preconditions.checkNotNull(user.id(), "user id");
    this.name = user.name();
    this.externalId = Optional.ofNullable(user.externalId());
    this.enabled = user.enabled();
  }

  /** Returns the Gravitino-assigned id of the user. */
  public Long id() {
    return id;
  }

  /** Returns the name of the user. */
  public String name() {
    return name;
  }

  /** Returns the external identifier of the user, or empty if not set. */
  public Optional<String> externalId() {
    return externalId;
  }

  /** Returns whether the user is enabled. */
  public boolean enabled() {
    return enabled;
  }
}
