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
package org.apache.gravitino.iceberg.service;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequestParser;

public class IcebergObjectMapper {
  private static final ObjectMapper objectMapper = createIcebergObjectMapper();

  private IcebergObjectMapper() {}

  private static ObjectMapper createIcebergObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);
    // Registered after Iceberg's serializers, so it takes precedence for the same type.
    mapper.registerModule(
        new SimpleModule("gravitino-iceberg-scan-request")
            .addDeserializer(PlanTableScanRequest.class, new PlanTableScanRequestDeserializer()));
    return mapper;
  }

  /**
   * The REST spec writes the constant expressions as {@code {"type": "true"}} and {@code {"type":
   * "false"}}, while Iceberg's parser accepts them only as JSON booleans. Both forms are accepted.
   */
  private static class PlanTableScanRequestDeserializer
      extends JsonDeserializer<PlanTableScanRequest> {
    @Override
    public PlanTableScanRequest deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      JsonNode node = parser.getCodec().readTree(parser);
      if (node instanceof ObjectNode && node.has("filter")) {
        ((ObjectNode) node).set("filter", normalizeConstants(node.get("filter")));
      }
      return PlanTableScanRequestParser.fromJson(node);
    }

    private static JsonNode normalizeConstants(JsonNode expression) {
      if (!(expression instanceof ObjectNode)) {
        return expression;
      }
      ObjectNode object = (ObjectNode) expression;
      JsonNode type = object.get("type");
      if (type != null && type.isTextual() && object.size() == 1) {
        if ("true".equals(type.asText())) {
          return BooleanNode.TRUE;
        }
        if ("false".equals(type.asText())) {
          return BooleanNode.FALSE;
        }
      }
      for (String child : new String[] {"left", "right", "child"}) {
        if (object.has(child)) {
          object.set(child, normalizeConstants(object.get(child)));
        }
      }
      return object;
    }
  }

  public static ObjectMapper getInstance() {
    return objectMapper;
  }
}
