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
package org.apache.gravitino.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.policy.TagValueSelector;

/** JSON serializer and deserializer for policy-to-tag selectors. */
public final class PolicyAssociationSelectorSerde {

  private static final String TYPE = "type";
  private static final String VALUE = "value";

  private static final ObjectMapper MAPPER =
      JsonUtils.anyFieldMapper()
          .copy()
          .registerModule(
              new SimpleModule()
                  .addSerializer(PolicyAssociationSelector.class, new Serializer())
                  .addDeserializer(PolicyAssociationSelector.class, new Deserializer()));

  private PolicyAssociationSelectorSerde() {}

  /**
   * Serializes a selector.
   *
   * @param selector The selector to serialize.
   * @return The selector JSON.
   */
  public static String serialize(PolicyAssociationSelector selector) {
    try {
      return MAPPER.writerFor(PolicyAssociationSelector.class).writeValueAsString(selector);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize policy association selector", e);
    }
  }

  /**
   * Deserializes a selector.
   *
   * @param json The selector JSON.
   * @return The selector.
   */
  public static PolicyAssociationSelector deserialize(String json) {
    try {
      return MAPPER.readValue(json, PolicyAssociationSelector.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to deserialize policy association selector", e);
    }
  }

  /** Serializes a policy association selector. */
  public static final class Serializer extends JsonSerializer<PolicyAssociationSelector> {

    @Override
    public void serialize(
        PolicyAssociationSelector selector,
        JsonGenerator generator,
        SerializerProvider serializerProvider)
        throws IOException {
      if (!(selector instanceof AllValuesSelector) && !(selector instanceof TagValueSelector)) {
        throw JsonMappingException.from(
            generator, "Unsupported policy association selector: " + selector);
      }

      generator.writeStartObject();
      generator.writeStringField(TYPE, selector.type());
      if (selector instanceof TagValueSelector) {
        generator.writeStringField(VALUE, ((TagValueSelector) selector).value());
      }
      generator.writeEndObject();
    }
  }

  /** Deserializes a policy association selector. */
  public static final class Deserializer extends JsonDeserializer<PolicyAssociationSelector> {

    @Override
    public PolicyAssociationSelector deserialize(
        JsonParser parser, DeserializationContext deserializationContext) throws IOException {
      JsonNode node = parser.getCodec().readTree(parser);
      String type = node.get(TYPE).asText();
      if (AllValuesSelector.TYPE.equals(type)) {
        return AllValuesSelector.get();
      }
      if (TagValueSelector.TYPE.equals(type)) {
        return TagValueSelector.of(node.get(VALUE).asText());
      }
      throw JsonMappingException.from(
          parser, "Unsupported policy association selector type: " + type);
    }
  }
}
