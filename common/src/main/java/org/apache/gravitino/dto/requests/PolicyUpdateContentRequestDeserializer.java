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
import java.io.IOException;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest.UpdatePolicyContentRequest;

/**
 * Deserializes {@link UpdatePolicyContentRequest}, resolving {@code policyType} before reading
 * {@code newContent} so mis-cased or unknown built-in types are reported instead of failing as
 * malformed custom content.
 */
public class PolicyUpdateContentRequestDeserializer
    extends JsonDeserializer<UpdatePolicyContentRequest> {

  @Override
  public UpdatePolicyContentRequest deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException {
    ObjectMapper mapper = (ObjectMapper) p.getCodec();
    JsonNode node = mapper.readTree(p);
    String policyType = textOrNull(node.get("policyType"));
    Class<? extends PolicyContentDTO> contentClass;
    try {
      contentClass = PolicyWireContentTypes.contentClassFor(policyType);
    } catch (IllegalArgumentException e) {
      // Wrap so Jersey's JsonMappingExceptionMapper returns 400 with this message.
      throw JsonMappingException.from(p, e.getMessage(), e);
    }

    PolicyContentDTO content = null;
    JsonNode contentNode = node.get("newContent");
    if (contentNode != null && !contentNode.isNull()) {
      content = mapper.treeToValue(contentNode, contentClass);
    }

    return new UpdatePolicyContentRequest(policyType, content);
  }

  private static String textOrNull(JsonNode node) {
    return node == null || node.isNull() ? null : node.asText();
  }
}
