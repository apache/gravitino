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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.idp.storage.po.IdpGroupWithUsersPO;
import org.apache.gravitino.idp.storage.po.IdpUserWithGroupsPO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.utils.PrincipalUtils;

/** Converts built-in IdP persistence objects to domain models. */
public final class IdpPOConverters {

  private IdpPOConverters() {}

  /**
   * Serializes audit information for persistence.
   *
   * @param auditInfo audit information
   * @return JSON string
   */
  public static String toAuditInfoJson(AuditInfo auditInfo) {
    Preconditions.checkNotNull(auditInfo, "auditInfo must not be null");
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(auditInfo);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize IdP audit info", e);
    }
  }

  /**
   * Deserializes audit information from persistence.
   *
   * @param json JSON string, blank means empty audit
   * @return audit information
   */
  public static AuditInfo fromAuditInfoJson(String json) {
    if (StringUtils.isBlank(json)) {
      return AuditInfo.EMPTY;
    }
    try {
      return JsonUtils.anyFieldMapper().readValue(json, AuditInfo.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize IdP audit info", e);
    }
  }

  /**
   * Builds create-time audit JSON for the current principal.
   *
   * @return serialized audit info
   */
  public static String newCreateAuditInfoJson() {
    return toAuditInfoJson(
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentUserName())
            .withCreateTime(Instant.now())
            .build());
  }

  /**
   * Rebuilds audit JSON with last-modifier fields for the current principal.
   *
   * @param existingAuditInfoJson previously stored audit JSON
   * @return updated audit JSON
   */
  public static String withLastModifiedAuditInfoJson(String existingAuditInfoJson) {
    AuditInfo existing = fromAuditInfoJson(existingAuditInfoJson);
    return toAuditInfoJson(
        AuditInfo.builder()
            .withCreator(existing.creator())
            .withCreateTime(existing.createTime())
            .withLastModifier(PrincipalUtils.getCurrentUserName())
            .withLastModifiedTime(Instant.now())
            .build());
  }

  /**
   * Converts a joined user row to a built-in IdP user.
   *
   * @param userPO The joined user row.
   * @return The built-in IdP user.
   */
  public static IdpUser fromIdpUserWithGroupsPO(IdpUserWithGroupsPO userPO) {
    Preconditions.checkNotNull(userPO, "userPO must not be null");
    List<String> groupNames = parseJsonStringList(userPO.getGroupNames());
    boolean enabled = userPO.getEnabled() == null || userPO.getEnabled();
    AuditInfo auditInfo = fromAuditInfoJson(userPO.getAuditInfo());
    if (StringUtils.isBlank(userPO.getPasswordHash())) {
      return new IdpUser(userPO.getName(), groupNames, enabled, auditInfo);
    }
    return new IdpUser(userPO.getName(), userPO.getPasswordHash(), groupNames, enabled, auditInfo);
  }

  /**
   * Converts a joined group row to a built-in IdP group.
   *
   * @param groupPO The joined group row.
   * @return The built-in IdP group.
   */
  public static IdpGroup fromIdpGroupWithUsersPO(IdpGroupWithUsersPO groupPO) {
    Preconditions.checkNotNull(groupPO, "groupPO must not be null");
    return new IdpGroup(
        groupPO.getName(),
        parseJsonStringList(groupPO.getUsernames()),
        groupPO.getComment(),
        fromAuditInfoJson(groupPO.getAuditInfo()));
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
