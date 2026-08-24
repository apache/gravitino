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
package org.apache.gravitino.policy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nullable;
import org.apache.gravitino.json.JsonUtils;

/** Serializes policy-to-tag selectors into their canonical relation-edge JSON representation. */
public final class PolicyTagSelectorSerde {

  private PolicyTagSelectorSerde() {}

  /**
   * Serializes a selector using the canonical selector JSON field order.
   *
   * @param selector The selector, or null for tag-presence matching.
   * @return The canonical JSON string, or null for tag-presence matching.
   */
  @Nullable
  public static String serialize(@Nullable PolicyTagSelector selector) {
    if (selector == null) {
      return null;
    }

    ObjectNode node = JsonUtils.anyFieldMapper().createObjectNode();
    node.put("type", selector.type().name());
    node.put("value", selector.value());
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize policy tag selector", e);
    }
  }

  /**
   * Deserializes a canonical selector JSON string.
   *
   * @param json The selector JSON, or null for tag-presence matching.
   * @return The selector, or null for tag-presence matching.
   */
  @Nullable
  public static PolicyTagSelector deserialize(@Nullable String json) {
    if (json == null) {
      return null;
    }

    try {
      JsonNode node = JsonUtils.anyFieldMapper().readTree(json);
      if (!node.isObject()) {
        throw new IllegalArgumentException("Policy tag selector must be a JSON object");
      }
      JsonNode type = node.get("type");
      JsonNode value = node.get("value");
      if (type == null || !type.isTextual()) {
        throw new IllegalArgumentException("Policy tag selector type must be a string");
      }
      if (!PolicyTagSelector.Type.TAG_VALUE.name().equals(type.textValue())) {
        throw new IllegalArgumentException("Unsupported policy tag selector type: " + type);
      }
      if (value == null || !value.isTextual()) {
        throw new IllegalArgumentException("TAG_VALUE selector value must be a string");
      }
      return PolicyTagSelector.tagValue(value.textValue());
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to deserialize policy tag selector", e);
    }
  }
}
