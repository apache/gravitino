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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.policy.Policy;

/**
 * Resolves {@code policyType} type ids for policy content polymorphism.
 *
 * <p>Known ids (built-ins and {@code custom}) are matched case-insensitively before content fields
 * are bound. Unknown {@code system_*} names and any other unregistered id fail with a type-named
 * error; there is no fallback that treats unmatched ids as custom content.
 */
public class PolicyContentTypeIdResolver extends TypeIdResolverBase {

  private static final Map<String, Class<? extends PolicyContentDTO>> CONTENT_BY_TYPE =
      ImmutableMap.<String, Class<? extends PolicyContentDTO>>builder()
          .put("system_iceberg_compaction", PolicyContentDTO.IcebergCompactionContentDTO.class)
          .put(
              "system_iceberg_orphan_file_removal",
              PolicyContentDTO.IcebergOrphanFileRemovalContentDTO.class)
          .put(
              "system_iceberg_rewrite_manifests",
              PolicyContentDTO.IcebergRewriteManifestsContentDTO.class)
          .put(
              "system_iceberg_snapshot_expiration",
              PolicyContentDTO.IcebergSnapshotExpirationContentDTO.class)
          .put("system_row_filter", PolicyContentDTO.RowFilterContentDTO.class)
          .put("system_column_mask", PolicyContentDTO.ColumnMaskContentDTO.class)
          .put("system_access_control", PolicyContentDTO.AccessControlContentDTO.class)
          .put("custom", PolicyContentDTO.CustomContentDTO.class)
          .build();

  private static final Map<Class<? extends PolicyContentDTO>, String> TYPE_BY_CONTENT_CLASS;

  static {
    ImmutableMap.Builder<Class<? extends PolicyContentDTO>, String> reverse =
        ImmutableMap.builder();
    for (Map.Entry<String, Class<? extends PolicyContentDTO>> entry : CONTENT_BY_TYPE.entrySet()) {
      reverse.put(entry.getValue(), entry.getKey());
    }
    TYPE_BY_CONTENT_CLASS = reverse.build();
  }

  private JavaType baseType;

  @Override
  public void init(JavaType bt) {
    this.baseType = bt;
  }

  @Override
  public String idFromValue(Object value) {
    return idFromValueAndType(value, value.getClass());
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    Class<?> type = suggestedType != null ? suggestedType : value.getClass();
    String id = typeIdForContentClass(type);
    if (id == null) {
      throw new IllegalArgumentException("Unknown policy content class: " + type.getName());
    }
    return id;
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) throws IOException {
    try {
      return context.constructSpecializedType(baseType, contentClassFor(id));
    } catch (IllegalArgumentException e) {
      // Wrap so Jersey's JsonMappingExceptionMapper can return HTTP 400 with this message.
      throw JsonMappingException.from((JsonParser) null, e.getMessage(), e);
    }
  }

  @Override
  public String getDescForKnownTypeIds() {
    return "Gravitino policyType content ids";
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.NAME;
  }

  private static Class<? extends PolicyContentDTO> contentClassFor(String policyType) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(policyType), "\"policyType\" is required and cannot be empty");
    Class<? extends PolicyContentDTO> exact = CONTENT_BY_TYPE.get(policyType);
    if (exact != null) {
      return exact;
    }
    for (Map.Entry<String, Class<? extends PolicyContentDTO>> entry : CONTENT_BY_TYPE.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(policyType)) {
        return entry.getValue();
      }
    }
    if (policyType.startsWith(Policy.BUILT_IN_TYPE_PREFIX)) {
      throw new IllegalArgumentException(
          String.format("Unknown built-in policy type: %s", policyType));
    }
    throw new IllegalArgumentException(
        String.format(
            "Unknown policy type: %s, it should start with '%s' or be 'custom'",
            policyType, Policy.BUILT_IN_TYPE_PREFIX));
  }

  private static String typeIdForContentClass(Class<?> contentClass) {
    if (contentClass == null) {
      return null;
    }
    String exact = TYPE_BY_CONTENT_CLASS.get(contentClass);
    if (exact != null) {
      return exact;
    }
    for (Map.Entry<Class<? extends PolicyContentDTO>, String> entry :
        TYPE_BY_CONTENT_CLASS.entrySet()) {
      if (entry.getKey().isAssignableFrom(contentClass)) {
        return entry.getValue();
      }
    }
    return null;
  }
}
