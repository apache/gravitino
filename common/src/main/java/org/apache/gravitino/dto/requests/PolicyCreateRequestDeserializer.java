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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import org.apache.gravitino.dto.policy.PolicyContentDTO;

/**
 * Deserializes {@link PolicyCreateRequest}, resolving {@code policyType} before reading {@code
 * content} so mis-cased or unknown built-in types are reported instead of failing as malformed
 * custom content.
 */
public class PolicyCreateRequestDeserializer extends JsonDeserializer<PolicyCreateRequest> {

  private static final Set<String> KNOWN_PROPERTIES =
      ImmutableSet.of("name", "comment", "policyType", "enabled", "content");

  @Override
  public PolicyCreateRequest deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException {
    ObjectMapper mapper = (ObjectMapper) p.getCodec();
    JsonNode node = mapper.readTree(p);
    rejectUnknownProperties(p, node);
    String policyType = textOrNull(node.get("policyType"));
    Class<? extends PolicyContentDTO> contentClass;
    try {
      contentClass = PolicyWireContentTypes.contentClassFor(policyType);
    } catch (IllegalArgumentException e) {
      // Wrap so Jersey's JsonMappingExceptionMapper returns 400 with this message.
      throw JsonMappingException.from(p, e.getMessage(), e);
    }

    PolicyContentDTO content = null;
    JsonNode contentNode = node.get("content");
    if (contentNode != null && !contentNode.isNull()) {
      content = mapper.treeToValue(contentNode, contentClass);
    }

    return new PolicyCreateRequest(
        textOrNull(node.get("name")),
        policyType,
        textOrNull(node.get("comment")),
        readEnabled(mapper, p, node.get("enabled")),
        content);
  }

  /**
   * Rejects properties that are not part of {@link PolicyCreateRequest}. Reading the tree and
   * picking known keys would otherwise ignore typos such as {@code enabld}.
   *
   * @param p the parser used to build the mapping exception
   * @param node the request object
   * @throws JsonMappingException if {@code node} is not an object or contains an unknown property
   */
  private static void rejectUnknownProperties(JsonParser p, JsonNode node)
      throws JsonMappingException {
    if (node == null || !node.isObject()) {
      throw JsonMappingException.from(
          p, "Cannot deserialize PolicyCreateRequest: request must be a JSON object");
    }
    Iterator<String> fields = node.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      if (!KNOWN_PROPERTIES.contains(field)) {
        throw JsonMappingException.from(
            p,
            String.format(
                "Unrecognized field \"%s\" (class %s), not marked as ignorable",
                field, PolicyCreateRequest.class.getName()));
      }
    }
  }

  /**
   * Binds {@code enabled} with Jackson's {@link Boolean} deserializer. {@link JsonNode#asBoolean()}
   * coerces non-booleans such as {@code "typo"}, objects, and arrays to {@code false}.
   *
   * @param mapper the mapper that owns Boolean binding rules
   * @param p the parser used to build the mapping exception
   * @param enabledNode the {@code enabled} field, or null when omitted
   * @return the bound value, or {@code true} when the field is absent or null
   * @throws IOException if the value is not a JSON boolean
   */
  private static boolean readEnabled(ObjectMapper mapper, JsonParser p, JsonNode enabledNode)
      throws IOException {
    if (enabledNode == null || enabledNode.isNull()) {
      return true;
    }
    Boolean enabled = mapper.treeToValue(enabledNode, Boolean.class);
    if (enabled == null) {
      throw JsonMappingException.from(p, "\"enabled\" must be a boolean");
    }
    return enabled;
  }

  private static String textOrNull(JsonNode node) {
    return node == null || node.isNull() ? null : node.asText();
  }
}
