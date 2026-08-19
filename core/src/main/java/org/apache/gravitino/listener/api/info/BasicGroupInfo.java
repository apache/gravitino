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
import org.apache.gravitino.authorization.BasicGroup;

/** Provides read-only access to basic group information for event listeners. */
@DeveloperApi
public class BasicGroupInfo {
  private final Long id;
  private final String name;
  private final Optional<String> externalId;

  /**
   * Construct a new {@link BasicGroupInfo} instance with the given {@link BasicGroup} information.
   *
   * @param group the {@link BasicGroup} instance.
   */
  public BasicGroupInfo(BasicGroup group) {
    this.id = Preconditions.checkNotNull(group.id(), "group id");
    this.name = group.name();
    this.externalId = Optional.ofNullable(group.externalId());
  }

  /** Returns the Gravitino-assigned id of the group. */
  public Long id() {
    return id;
  }

  /** Returns the name of the group. */
  public String name() {
    return name;
  }

  /** Returns the external identifier of the group, or empty if not set. */
  public Optional<String> externalId() {
    return externalId;
  }
}
