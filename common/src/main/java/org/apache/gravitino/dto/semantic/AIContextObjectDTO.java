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
package org.apache.gravitino.dto.semantic;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.AIContextObject;

/** DTO for structured Semantic Model AI context. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"instructions", "synonyms", "examples"})
@JsonDeserialize(using = AIContextObjectDTO.Deserializer.class)
public class AIContextObjectDTO {

  @Nullable
  @JsonProperty("instructions")
  private String instructions;

  @Nullable
  @JsonProperty("synonyms")
  private String[] synonyms;

  @Nullable
  @JsonProperty("examples")
  private String[] examples;

  @JsonIgnore
  @Getter(AccessLevel.NONE)
  @Builder.Default
  private Map<String, Object> additionalProperties = new LinkedHashMap<>();

  /**
   * Creates a structured AI context DTO from an API model.
   *
   * @param aiContextObject The API AI context object.
   * @return The structured AI context DTO.
   */
  public static AIContextObjectDTO fromAIContextObject(AIContextObject aiContextObject) {
    return builder()
        .withInstructions(aiContextObject.instructions())
        .withSynonyms(aiContextObject.synonyms())
        .withExamples(aiContextObject.examples())
        .withAdditionalProperties(new LinkedHashMap<>(aiContextObject.additionalProperties()))
        .build();
  }

  /**
   * Converts this DTO to an API AI context object.
   *
   * @return The API AI context object.
   */
  public AIContextObject toAIContextObject() {
    return AIContextObject.builder()
        .withInstructions(instructions)
        .withSynonyms(synonyms)
        .withExamples(examples)
        .withAdditionalProperties(
            additionalProperties == null ? Collections.emptyMap() : additionalProperties)
        .build();
  }

  /**
   * Returns unknown AI-context properties in their input order.
   *
   * @return The additional properties.
   */
  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return additionalProperties == null ? Collections.emptyMap() : additionalProperties;
  }

  /** Deserializes structured AI context while retaining unknown JSON values and their order. */
  public static final class Deserializer extends JsonDeserializer<AIContextObjectDTO> {

    @Override
    public AIContextObjectDTO deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      if (!parser.hasToken(JsonToken.START_OBJECT)) {
        throw JsonMappingException.from(parser, "Structured AI context must be an object");
      }

      String instructions = null;
      String[] synonyms = null;
      String[] examples = null;
      Map<String, Object> additionalProperties = new LinkedHashMap<>();
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        if (!parser.hasToken(JsonToken.FIELD_NAME)) {
          throw JsonMappingException.from(parser, "Expected an AI context property name");
        }
        String name = parser.currentName();
        JsonToken valueToken = parser.nextToken();
        switch (name) {
          case "instructions":
            instructions = readString(parser, valueToken, name);
            break;
          case "synonyms":
            synonyms = readStringArray(parser, valueToken, name);
            break;
          case "examples":
            examples = readStringArray(parser, valueToken, name);
            break;
          default:
            additionalProperties.put(name, readJsonValue(parser, valueToken));
        }
      }

      return AIContextObjectDTO.builder()
          .withInstructions(instructions)
          .withSynonyms(synonyms)
          .withExamples(examples)
          .withAdditionalProperties(additionalProperties)
          .build();
    }

    private static String readString(JsonParser parser, JsonToken token, String name)
        throws IOException {
      if (token != JsonToken.VALUE_STRING) {
        throw JsonMappingException.from(parser, name + " must be a string");
      }
      return parser.getText();
    }

    private static String[] readStringArray(JsonParser parser, JsonToken token, String name)
        throws IOException {
      if (token != JsonToken.START_ARRAY) {
        throw JsonMappingException.from(parser, name + " must be an array of strings");
      }
      List<String> values = new ArrayList<>();
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        values.add(readString(parser, parser.currentToken(), name));
      }
      return values.toArray(new String[0]);
    }

    @Nullable
    private static Object readJsonValue(JsonParser parser, JsonToken token) throws IOException {
      switch (token) {
        case VALUE_NULL:
          return null;
        case VALUE_STRING:
          return parser.getText();
        case VALUE_TRUE:
          return Boolean.TRUE;
        case VALUE_FALSE:
          return Boolean.FALSE;
        case VALUE_NUMBER_INT:
          return parser.getBigIntegerValue();
        case VALUE_NUMBER_FLOAT:
          return parser.getDecimalValue();
        case START_OBJECT:
          return readJsonObject(parser);
        case START_ARRAY:
          return readJsonArray(parser);
        default:
          throw JsonMappingException.from(parser, "Unsupported AI context JSON token: " + token);
      }
    }

    private static Map<String, Object> readJsonObject(JsonParser parser) throws IOException {
      Map<String, Object> values = new LinkedHashMap<>();
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        if (!parser.hasToken(JsonToken.FIELD_NAME)) {
          throw JsonMappingException.from(parser, "Expected an AI context object property name");
        }
        String name = parser.currentName();
        values.put(name, readJsonValue(parser, parser.nextToken()));
      }
      return values;
    }

    private static List<Object> readJsonArray(JsonParser parser) throws IOException {
      List<Object> values = new ArrayList<>();
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        values.add(readJsonValue(parser, parser.currentToken()));
      }
      return values;
    }
  }
}
