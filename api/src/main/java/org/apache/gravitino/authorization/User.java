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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/** The interface of a user. The user is the entity which executes every operation. */
@Evolving
public interface User extends Auditable {

  /**
   * The name of the user. It's the identifier of User. It must be unique. Usually the name comes
   * from an external user management system like LDAP, IAM and so on.
   *
   * @return The name of the user.
   */
  String name();

  /**
   * The stable identifier assigned by an upstream identity system (for example, SCIM, LDAP, or
   * IAM), or null if not set.
   *
   * <p>Gravitino {@link User#name() user names} may differ from upstream ids or be unknown at sync
   * time. External id lets integrators look up, enable/disable, and delete users without relying on
   * the Gravitino user name.
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

  /**
   * The roles of the user. A user can have multiple roles. Every role binds several privileges.
   *
   * @return The roles of the user.
   */
  List<String> roles();
}
