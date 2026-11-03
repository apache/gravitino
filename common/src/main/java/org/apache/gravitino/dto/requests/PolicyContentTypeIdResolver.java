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
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.policy.Policy;

/**
 * Selects the policy content DTO from the sibling {@code policyType}, matching known types
 * case-insensitively via {@link Policy.BuiltInType#fromPolicyType(String)}.
 */
public class PolicyContentTypeIdResolver extends TypeIdResolverBase {

  private static final Map<Policy.BuiltInType, Class<? extends PolicyContentDTO>> CONTENT_CLASSES =
      ImmutableMap.<Policy.BuiltInType, Class<? extends PolicyContentDTO>>builder()
          .put(
              Policy.BuiltInType.ICEBERG_COMPACTION,
              PolicyContentDTO.IcebergCompactionContentDTO.class)
          .put(
              Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL,
              PolicyContentDTO.IcebergOrphanFileRemovalContentDTO.class)
          .put(
              Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
              PolicyContentDTO.IcebergRewriteManifestsContentDTO.class)
          .put(
              Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
              PolicyContentDTO.IcebergSnapshotExpirationContentDTO.class)
          .put(Policy.BuiltInType.ROW_FILTER, PolicyContentDTO.RowFilterContentDTO.class)
          .put(Policy.BuiltInType.COLUMN_MASK, PolicyContentDTO.ColumnMaskContentDTO.class)
          .put(Policy.BuiltInType.ACCESS_CONTROL, PolicyContentDTO.AccessControlContentDTO.class)
          .put(Policy.BuiltInType.CUSTOM, PolicyContentDTO.CustomContentDTO.class)
          .build();

  private JavaType baseType;

  @Override
  public void init(JavaType baseType) {
    this.baseType = baseType;
  }

  @Override
  public String idFromValue(Object value) {
    return idFromValueAndType(value, value.getClass());
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    for (Map.Entry<Policy.BuiltInType, Class<? extends PolicyContentDTO>> entry :
        CONTENT_CLASSES.entrySet()) {
      if (entry.getValue().isAssignableFrom(suggestedType)) {
        return entry.getKey().policyType();
      }
    }
    throw new IllegalArgumentException("Unknown policy content class: " + suggestedType);
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) throws IOException {
    try {
      Policy.BuiltInType type = Policy.BuiltInType.fromPolicyType(id);
      Class<? extends PolicyContentDTO> contentClass = CONTENT_CLASSES.get(type);
      if (contentClass == null) {
        throw new IllegalArgumentException("Unsupported policy type: " + id);
      }
      return context.constructSpecializedType(baseType, contentClass);
    } catch (IllegalArgumentException e) {
      // JsonMappingExceptionMapper turns this into HTTP 400 carrying the type-named message.
      throw JsonMappingException.from((JsonParser) null, e.getMessage(), e);
    }
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.NAME;
  }
}
