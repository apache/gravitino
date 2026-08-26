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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.AIContextObject;

/** DTO for structured Semantic Model AI context. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"instructions", "synonyms", "examples"})
@JsonDeserialize(using = AIContextObjectDTO.Deserializer.class)
public class AIContextObjectDTO {

  @Nullable
  @JsonProperty("instructions")
  private String instructions;

  @Nullable
  @JsonProperty("synonyms")
  @Getter(AccessLevel.NONE)
  private String[] synonyms;

  @Nullable
  @JsonProperty("examples")
  @Getter(AccessLevel.NONE)
  private String[] examples;

  @JsonIgnore
  @Getter(AccessLevel.NONE)
  private Map<String, Object> additionalProperties = new LinkedHashMap<>();

  @Builder(setterPrefix = "with")
  private AIContextObjectDTO(
      @Nullable String instructions,
      @Nullable String[] synonyms,
      @Nullable String[] examples,
      @Nullable Map<String, Object> additionalProperties) {
    AIContextObject normalized =
        AIContextObject.builder()
            .withInstructions(instructions)
            .withSynonyms(synonyms)
            .withExamples(examples)
            .withAdditionalProperties(
                additionalProperties == null ? Collections.emptyMap() : additionalProperties)
            .build();
    this.instructions = normalized.instructions();
    this.synonyms = normalized.synonyms();
    this.examples = normalized.examples();
    this.additionalProperties = normalized.additionalProperties();
  }

  /**
   * Returns alternative names and terms.
   *
   * @return A defensive copy of the synonyms, or {@code null} when not provided.
   */
  @Nullable
  public String[] getSynonyms() {
    return SemanticDTOUtils.copyArray(synonyms);
  }

  /**
   * Returns sample questions or use cases.
   *
   * @return A defensive copy of the examples, or {@code null} when not provided.
   */
  @Nullable
  public String[] getExamples() {
    return SemanticDTOUtils.copyArray(examples);
  }

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
   * <p>The returned map and all nested maps and lists are unmodifiable.
   *
   * @return The deeply immutable additional properties.
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
        throw JsonMappingException.from(
            parser, "Structured AI context must be an object, but found " + parser.currentToken());
      }

      String instructions = null;
      String[] synonyms = null;
      String[] examples = null;
      Map<String, Object> additionalProperties = new LinkedHashMap<>();
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        if (!parser.hasToken(JsonToken.FIELD_NAME)) {
          throw JsonMappingException.from(
              parser, "Expected an AI context property name, but found " + parser.currentToken());
        }
        String name = parser.currentName();
        JsonToken valueToken = parser.nextToken();
        switch (name) {
          case "instructions":
            instructions = readNullableString(parser, valueToken, name);
            break;
          case "synonyms":
            synonyms = readNullableStringArray(parser, valueToken, name);
            break;
          case "examples":
            examples = readNullableStringArray(parser, valueToken, name);
            break;
          default:
            additionalProperties.put(name, SemanticDTOUtils.readJsonValue(parser, valueToken));
        }
      }

      return AIContextObjectDTO.builder()
          .withInstructions(instructions)
          .withSynonyms(synonyms)
          .withExamples(examples)
          .withAdditionalProperties(additionalProperties)
          .build();
    }

    @Nullable
    private static String readNullableString(JsonParser parser, JsonToken token, String name)
        throws IOException {
      if (token == JsonToken.VALUE_NULL) {
        return null;
      }
      return readRequiredString(parser, token, name);
    }

    @Nullable
    private static String[] readNullableStringArray(JsonParser parser, JsonToken token, String name)
        throws IOException {
      if (token == JsonToken.VALUE_NULL) {
        return null;
      }
      if (token != JsonToken.START_ARRAY) {
        throw JsonMappingException.from(parser, name + " must be an array of strings");
      }
      List<String> values = new ArrayList<>();
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        values.add(readRequiredString(parser, parser.currentToken(), name));
      }
      return values.toArray(new String[0]);
    }

    private static String readRequiredString(JsonParser parser, JsonToken token, String name)
        throws IOException {
      if (token != JsonToken.VALUE_STRING) {
        throw JsonMappingException.from(parser, name + " must be a string");
      }
      return parser.getText();
    }
  }
}
