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
package org.apache.gravitino.dto.secret;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.gravitino.secret.SecretReference;

/** Data transfer object for an external {@link SecretReference}. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(setterPrefix = "with")
@ToString
public class SecretReferenceDTO {

  @JsonProperty("provider")
  private String provider;

  @Builder.Default
  @JsonProperty("attributes")
  private Map<String, String> attributes = ImmutableMap.of();

  /**
   * Converts this DTO to a {@link SecretReference}.
   *
   * @return the secret reference
   */
  public SecretReference toSecretReference() {
    Map<String, String> attrs = attributes == null ? ImmutableMap.of() : attributes;
    return new SecretReference(provider, attrs);
  }

  /**
   * Creates a DTO from a {@link SecretReference}.
   *
   * @param reference the secret reference
   * @return the secret reference DTO
   */
  public static SecretReferenceDTO fromSecretReference(SecretReference reference) {
    return new SecretReferenceDTO(reference.provider(), reference.attributes());
  }

  /**
   * Converts a property-key map of DTOs to {@link SecretReference}s.
   *
   * @param dtos property key → reference DTO; {@code null} or empty returns an empty map
   * @return property key → reference (never {@code null})
   */
  public static Map<String, SecretReference> toSecretReferences(
      @Nullable Map<String, SecretReferenceDTO> dtos) {
    if (dtos == null || dtos.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, SecretReference> references = ImmutableMap.builder();
    for (Map.Entry<String, SecretReferenceDTO> entry : dtos.entrySet()) {
      String key = entry.getKey();
      SecretReferenceDTO dto = entry.getValue();
      if (dto == null) {
        throw new IllegalArgumentException("secretReferences[\"" + key + "\"] must not be null");
      }
      references.put(key, dto.toSecretReference());
    }
    return references.build();
  }

  /**
   * Converts a property-key map of {@link SecretReference}s to DTOs.
   *
   * @param references property key → reference; {@code null} or empty returns an empty map
   * @return property key → reference DTO (never {@code null})
   */
  public static Map<String, SecretReferenceDTO> fromSecretReferences(
      @Nullable Map<String, SecretReference> references) {
    if (references == null || references.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, SecretReferenceDTO> dtos = ImmutableMap.builder();
    for (Map.Entry<String, SecretReference> entry : references.entrySet()) {
      dtos.put(entry.getKey(), fromSecretReference(entry.getValue()));
    }
    return dtos.build();
  }
}
