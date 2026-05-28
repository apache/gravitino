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
package org.apache.gravitino.idp.storage.relational.utils;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.idp.storage.po.IdpGroupWithUsersPO;
import org.apache.gravitino.idp.storage.po.IdpUserWithGroupsPO;
import org.apache.gravitino.json.JsonUtils;

/** Converts built-in IdP persistence objects to domain models. */
public final class IdpPOConverters {
  private IdpPOConverters() {}

  /**
   * Converts a joined user row to a built-in IdP user.
   *
   * @param userPO The joined user row.
   * @return The built-in IdP user.
   */
  public static IdpUser fromIdpUserWithGroupsPO(IdpUserWithGroupsPO userPO) {
    Preconditions.checkNotNull(userPO, "userPO must not be null");
    List<String> groupNames = parseJsonStringList(userPO.getGroupNames());
    if (StringUtils.isBlank(userPO.getPasswordHash())) {
      return new IdpUser(userPO.getName(), groupNames);
    }
    return new IdpUser(userPO.getName(), userPO.getPasswordHash(), groupNames);
  }

  /**
   * Converts a joined group row to a built-in IdP group.
   *
   * @param groupPO The joined group row.
   * @return The built-in IdP group.
   */
  public static IdpGroup fromIdpGroupWithUsersPO(IdpGroupWithUsersPO groupPO) {
    Preconditions.checkNotNull(groupPO, "groupPO must not be null");
    return new IdpGroup(groupPO.getName(), parseJsonStringList(groupPO.getUsernames()));
  }

  @SuppressWarnings("unchecked")
  private static List<String> parseJsonStringList(String json) {
    if (StringUtils.isBlank(json)) {
      return Collections.emptyList();
    }
    try {
      List<String> values = JsonUtils.anyFieldMapper().readValue(json, List.class);
      return values.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse built-in IdP JSON string list", e);
    }
  }
}
