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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.io.IOException;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.semantic.AIContext;

/** DTO for the string-or-object Semantic Model AI context union. */
@Getter
@EqualsAndHashCode
@JsonSerialize(using = AIContextDTO.Serializer.class)
@JsonDeserialize(using = AIContextDTO.Deserializer.class)
public class AIContextDTO {

  @Nullable private final String text;
  @Nullable private final AIContextObjectDTO object;

  private AIContextDTO(@Nullable String text, @Nullable AIContextObjectDTO object) {
    this.text = text;
    this.object = object;
  }

  /**
   * Creates a builder for an AI context DTO.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates an AI context DTO from an API model.
   *
   * @param aiContext The API AI context.
   * @return The AI context DTO.
   */
  public static AIContextDTO fromAIContext(AIContext aiContext) {
    if (aiContext.isText()) {
      return builder().withText(aiContext.text()).build();
    }
    return builder().withObject(AIContextObjectDTO.fromAIContextObject(aiContext.object())).build();
  }

  /**
   * Converts this DTO to an API AI context.
   *
   * @return The API AI context.
   */
  public AIContext toAIContext() {
    if (text != null) {
      return AIContext.of(text);
    }
    Preconditions.checkArgument(object != null, "AI context object must not be null");
    return AIContext.of(object.toAIContextObject());
  }

  /** Builder for {@link AIContextDTO}. */
  public static final class Builder {

    @Nullable private String text;
    @Nullable private AIContextObjectDTO object;

    private Builder() {}

    /**
     * Sets the string variant.
     *
     * @param text The string AI context.
     * @return This builder.
     */
    public Builder withText(@Nullable String text) {
      this.text = text;
      return this;
    }

    /**
     * Sets the object variant.
     *
     * @param object The structured AI context.
     * @return This builder.
     */
    public Builder withObject(@Nullable AIContextObjectDTO object) {
      this.object = object;
      return this;
    }

    /**
     * Builds an AI context DTO.
     *
     * @return The AI context DTO.
     */
    public AIContextDTO build() {
      Preconditions.checkArgument(
          (text == null) != (object == null),
          "AI context must contain exactly one of text or object");
      return new AIContextDTO(text, object);
    }
  }

  /** Serializes the AI context union as its contained string or object. */
  public static final class Serializer extends JsonSerializer<AIContextDTO> {

    @Override
    public void serialize(
        AIContextDTO value, JsonGenerator generator, SerializerProvider serializers)
        throws IOException {
      if (value.text != null && value.object == null) {
        generator.writeString(value.text);
      } else if (value.text == null && value.object != null) {
        generator.writeObject(value.object);
      } else {
        throw JsonMappingException.from(
            generator, "AI context must contain exactly one of text or object");
      }
    }
  }

  /** Deserializes an AI context union from a string or object. */
  public static final class Deserializer extends JsonDeserializer<AIContextDTO> {

    @Override
    public AIContextDTO deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      JsonToken token = parser.currentToken();
      if (token == null) {
        token = parser.nextToken();
      }
      if (token == JsonToken.VALUE_STRING) {
        return builder().withText(parser.getText()).build();
      }
      if (token == JsonToken.START_OBJECT) {
        AIContextObjectDTO object = parser.getCodec().readValue(parser, AIContextObjectDTO.class);
        return builder().withObject(object).build();
      }
      throw JsonMappingException.from(parser, "AI context must be a string or object");
    }
  }
}
