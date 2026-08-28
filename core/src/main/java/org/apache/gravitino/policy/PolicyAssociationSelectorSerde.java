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
public final class PolicyAssociationSelectorSerde {

  private PolicyAssociationSelectorSerde() {}

  /**
   * Serializes a selector using the canonical selector JSON field order.
   *
   * @param selector The selector to serialize.
   * @return The canonical JSON string.
   */
  public static String serialize(PolicyAssociationSelector selector) {
    ObjectNode node = JsonUtils.anyFieldMapper().createObjectNode();
    node.put("type", selector.type());
    if (selector instanceof TagValueSelector) {
      node.put("value", ((TagValueSelector) selector).value());
    } else if (!(selector instanceof AllValuesSelector)) {
      throw new IllegalArgumentException("Unsupported policy association selector: " + selector);
    }
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize policy association selector", e);
    }
  }

  /**
   * Deserializes a canonical selector JSON string.
   *
   * <p>A null value is treated as {@link AllValuesSelector} for compatibility with relations
   * created before selectors became required.
   *
   * @param json The selector JSON, or null for a legacy tag-presence relation.
   * @return The deserialized selector.
   */
  public static PolicyAssociationSelector deserialize(@Nullable String json) {
    if (json == null) {
      return AllValuesSelector.get();
    }

    try {
      JsonNode node = JsonUtils.anyFieldMapper().readTree(json);
      if (!node.isObject()) {
        throw new IllegalArgumentException("Policy association selector must be a JSON object");
      }
      JsonNode type = node.get("type");
      if (type == null || !type.isTextual()) {
        throw new IllegalArgumentException("Policy association selector type must be a string");
      }
      if (AllValuesSelector.TYPE.equals(type.textValue())) {
        return AllValuesSelector.get();
      }
      if (!TagValueSelector.TYPE.equals(type.textValue())) {
        throw new IllegalArgumentException("Unsupported policy association selector type: " + type);
      }
      JsonNode value = node.get("value");
      if (value == null || !value.isTextual()) {
        throw new IllegalArgumentException("TAG_VALUE selector value must be a string");
      }
      return TagValueSelector.of(value.textValue());
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to deserialize policy association selector", e);
    }
  }
}
