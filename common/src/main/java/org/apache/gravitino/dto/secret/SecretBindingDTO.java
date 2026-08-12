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
import org.apache.gravitino.secret.SecretBinding;

/** Data transfer object for a write-through {@link SecretBinding}. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(setterPrefix = "with")
@ToString(exclude = "plaintext")
public class SecretBindingDTO {

  @JsonProperty("provider")
  private String provider;

  @JsonProperty("plaintext")
  private String plaintext;

  /**
   * Converts this DTO to a {@link SecretBinding}.
   *
   * @return the secret binding
   */
  public SecretBinding toSecretBinding() {
    return new SecretBinding(provider, plaintext);
  }

  /**
   * Creates a DTO from a {@link SecretBinding}.
   *
   * @param binding the secret binding
   * @return the secret binding DTO
   */
  public static SecretBindingDTO fromSecretBinding(SecretBinding binding) {
    return new SecretBindingDTO(binding.provider(), binding.plaintext());
  }

  /**
   * Converts a property-key map of DTOs to {@link SecretBinding}s.
   *
   * @param dtos property key → binding DTO; {@code null} or empty returns an empty map
   * @return property key → binding (never {@code null})
   */
  public static Map<String, SecretBinding> toSecretBindings(
      @Nullable Map<String, SecretBindingDTO> dtos) {
    if (dtos == null || dtos.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, SecretBinding> bindings = ImmutableMap.builder();
    for (Map.Entry<String, SecretBindingDTO> entry : dtos.entrySet()) {
      String key = entry.getKey();
      SecretBindingDTO dto = entry.getValue();
      if (dto == null) {
        throw new IllegalArgumentException("secretBindings[\"" + key + "\"] must not be null");
      }
      bindings.put(key, dto.toSecretBinding());
    }
    return bindings.build();
  }

  /**
   * Converts a property-key map of {@link SecretBinding}s to DTOs.
   *
   * @param bindings property key → binding; {@code null} or empty returns an empty map
   * @return property key → binding DTO (never {@code null})
   */
  public static Map<String, SecretBindingDTO> fromSecretBindings(
      @Nullable Map<String, SecretBinding> bindings) {
    if (bindings == null || bindings.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, SecretBindingDTO> dtos = ImmutableMap.builder();
    for (Map.Entry<String, SecretBinding> entry : bindings.entrySet()) {
      dtos.put(entry.getKey(), fromSecretBinding(entry.getValue()));
    }
    return dtos.build();
  }
}
