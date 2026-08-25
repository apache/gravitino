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
package org.apache.gravitino.storage.relational.po;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.CaseFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dimension;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/**
 * Serializes immutable Semantic Model API values for relational storage without depending on REST
 * DTOs.
 */
final class SemanticModelDefinitionSerDe {

  private static final ObjectMapper MAPPER = createMapper();

  private SemanticModelDefinitionSerDe() {}

  static String serialize(SemanticModelDefinition definition) throws JsonProcessingException {
    return MAPPER.writeValueAsString(definition);
  }

  static SemanticModelDefinition deserialize(String json) throws JsonProcessingException {
    return MAPPER.readValue(json, SemanticModelDefinition.class);
  }

  private static ObjectMapper createMapper() {
    SimpleModule module =
        new SimpleModule()
            .addSerializer(AIContext.class, new AIContextSerializer())
            .addDeserializer(AIContext.class, new AIContextDeserializer())
            .addSerializer(DataType.class, new DataTypeSerializer())
            .addDeserializer(DataType.class, new DataTypeDeserializer())
            .addSerializer(NameIdentifier.class, new JsonUtils.NameIdentifierSerializer())
            .addDeserializer(NameIdentifier.class, new JsonUtils.NameIdentifierDeserializer());

    ObjectMapper mapper =
        JsonUtils.anyFieldMapper()
            .copy()
            .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE)
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .registerModule(module);
    addBuilderMixIns(
        mapper,
        SemanticModelDefinition.class,
        SemanticModelDefinitionMixIn.class,
        SemanticModelDefinition.Builder.class);
    addBuilderMixIns(mapper, Dataset.class, DatasetMixIn.class, Dataset.Builder.class);
    addBuilderMixIns(
        mapper, Relationship.class, RelationshipMixIn.class, Relationship.Builder.class);
    addBuilderMixIns(mapper, Metric.class, MetricMixIn.class, Metric.Builder.class);
    addBuilderMixIns(mapper, Field.class, FieldMixIn.class, Field.Builder.class);
    addBuilderMixIns(mapper, Expression.class, ExpressionMixIn.class, Expression.Builder.class);
    addBuilderMixIns(
        mapper,
        DialectExpression.class,
        DialectExpressionMixIn.class,
        DialectExpression.Builder.class);
    addBuilderMixIns(mapper, Dimension.class, DimensionMixIn.class, Dimension.Builder.class);
    addBuilderMixIns(
        mapper, CustomExtension.class, CustomExtensionMixIn.class, CustomExtension.Builder.class);
    return mapper;
  }

  private static void addBuilderMixIns(
      ObjectMapper mapper, Class<?> valueClass, Class<?> mixInClass, Class<?> builderClass) {
    mapper.addMixIn(valueClass, mixInClass);
    mapper.addMixIn(builderClass, BuilderMixIn.class);
  }

  @JsonPOJOBuilder(withPrefix = "with")
  private abstract static class BuilderMixIn {}

  @JsonDeserialize(builder = SemanticModelDefinition.Builder.class)
  private abstract static class SemanticModelDefinitionMixIn {}

  @JsonDeserialize(builder = Dataset.Builder.class)
  private abstract static class DatasetMixIn {}

  @JsonDeserialize(builder = Relationship.Builder.class)
  private abstract static class RelationshipMixIn {}

  @JsonDeserialize(builder = Metric.Builder.class)
  private abstract static class MetricMixIn {}

  @JsonDeserialize(builder = Field.Builder.class)
  private abstract static class FieldMixIn {}

  @JsonDeserialize(builder = Expression.Builder.class)
  private abstract static class ExpressionMixIn {}

  @JsonDeserialize(builder = DialectExpression.Builder.class)
  private abstract static class DialectExpressionMixIn {}

  @JsonDeserialize(builder = Dimension.Builder.class)
  private abstract static class DimensionMixIn {}

  @JsonDeserialize(builder = CustomExtension.Builder.class)
  private abstract static class CustomExtensionMixIn {}

  private static final class AIContextSerializer extends JsonSerializer<AIContext> {

    @Override
    public void serialize(AIContext value, JsonGenerator generator, SerializerProvider serializers)
        throws IOException {
      if (value.isText()) {
        generator.writeString(value.text());
        return;
      }

      AIContextObject object = value.object();
      if (object == null) {
        throw JsonMappingException.from(generator, "Structured AI context must not be null");
      }
      generator.writeStartObject();
      writeOptionalField(generator, "instructions", object.instructions());
      writeOptionalField(generator, "synonyms", object.synonyms());
      writeOptionalField(generator, "examples", object.examples());
      for (Map.Entry<String, Object> entry : object.additionalProperties().entrySet()) {
        generator.writeObjectField(entry.getKey(), entry.getValue());
      }
      generator.writeEndObject();
    }

    private static void writeOptionalField(JsonGenerator generator, String name, Object value)
        throws IOException {
      if (value != null) {
        generator.writeObjectField(name, value);
      }
    }
  }

  private static final class AIContextDeserializer extends JsonDeserializer<AIContext> {

    @Override
    public AIContext deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      if (parser.hasToken(JsonToken.VALUE_STRING)) {
        return AIContext.of(parser.getText());
      }
      if (!parser.hasToken(JsonToken.START_OBJECT)) {
        throw JsonMappingException.from(parser, "AI context must be a string or object");
      }

      JsonNode root = parser.getCodec().readTree(parser);
      AIContextObject.Builder builder =
          AIContextObject.builder()
              .withInstructions(textOrNull(root.get("instructions")))
              .withSynonyms(stringArrayOrNull(root.get("synonyms"), parser))
              .withExamples(stringArrayOrNull(root.get("examples"), parser));

      Map<String, Object> additionalProperties = new LinkedHashMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        if (!isStandardAIContextProperty(entry.getKey())) {
          additionalProperties.put(
              entry.getKey(), parser.getCodec().treeToValue(entry.getValue(), Object.class));
        }
      }
      return AIContext.of(builder.withAdditionalProperties(additionalProperties).build());
    }

    private static String textOrNull(JsonNode value) {
      return value == null || value.isNull() ? null : value.textValue();
    }

    private static String[] stringArrayOrNull(JsonNode value, JsonParser parser)
        throws JsonProcessingException {
      return value == null || value.isNull()
          ? null
          : parser.getCodec().treeToValue(value, String[].class);
    }

    private static boolean isStandardAIContextProperty(String name) {
      return name.equals("instructions") || name.equals("synonyms") || name.equals("examples");
    }
  }

  private static final class DataTypeSerializer extends JsonSerializer<DataType> {

    @Override
    public void serialize(DataType value, JsonGenerator generator, SerializerProvider serializers)
        throws IOException {
      generator.writeString(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, value.name()));
    }
  }

  private static final class DataTypeDeserializer extends JsonDeserializer<DataType> {

    @Override
    public DataType deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      if (!parser.hasToken(JsonToken.VALUE_STRING)) {
        throw JsonMappingException.from(parser, "Semantic Model data type must be a string");
      }
      try {
        return DataType.valueOf(
            CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, parser.getText()));
      } catch (IllegalArgumentException e) {
        throw JsonMappingException.from(
            parser, "Unknown Semantic Model data type: " + parser.getText(), e);
      }
    }
  }
}
