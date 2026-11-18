/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.listener.api.event;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Shared customInfo keys and values for role assignment audit events. */
final class RoleAssignmentAuditInfos {

  static final String ROLE_NAMES = "roleNames";

  private RoleAssignmentAuditInfos() {}

  static Map<String, String> of(@Nullable List<String> roles) {
    if (roles == null || roles.isEmpty()) {
      return ImmutableMap.of();
    }
    return ImmutableMap.of(ROLE_NAMES, String.join(",", roles));
  }
}
