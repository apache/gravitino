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
package org.apache.gravitino.authorization;

import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/**
 * A lightweight view of a user backed only by {@code user_meta} row data, without role bindings.
 */
@Evolving
public interface BasicUser extends Auditable {

  /**
   * The name of the user.
   *
   * @return The name of the user.
   */
  String name();

  /**
   * The unique id assigned by Gravitino.
   *
   * @return The unique id of the user.
   */
  Long id();

  /**
   * The stable identifier assigned by an upstream identity system, or null if not set.
   *
   * @return The upstream external identifier, or null if not set.
   */
  @Nullable
  default String externalId() {
    return null;
  }

  /**
   * Whether the user is enabled.
   *
   * @return True if the user is enabled, false otherwise.
   */
  default boolean enabled() {
    return true;
  }
}
