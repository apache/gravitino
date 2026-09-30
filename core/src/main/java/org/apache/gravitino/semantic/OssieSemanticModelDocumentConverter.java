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
package org.apache.gravitino.semantic;

import static org.apache.gravitino.semantic.SemanticModel.DEFAULT_OSSIE_VERSION;
import static org.apache.gravitino.semantic.SemanticModel.PROPERTY_OSSIE_VERSION;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.requests.SemanticModelCreateRequest;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;

/** Converts between standalone Apache Ossie documents and Gravitino Semantic Models. */
public final class OssieSemanticModelDocumentConverter {

  /** Maximum accepted document length in characters. */
  public static final int MAX_DOCUMENT_LENGTH = 4 * 1024 * 1024;

  private static final String GRAVITINO_VENDOR = "GRAVITINO";
  private static final String INTERCHANGE_MARKER = "_apache_gravitino_interchange";
  private static final int INTERCHANGE_MARKER_VERSION = 1;
  private static final int MAX_NESTING_DEPTH = 100;
  private static final StreamReadConstraints STREAM_READ_CONSTRAINTS =
      StreamReadConstraints.builder()
          .maxNestingDepth(MAX_NESTING_DEPTH)
          .maxStringLength(MAX_DOCUMENT_LENGTH)
          .build();

  private static final Set<String> ROOT_PROPERTIES =
      Set.of(
          "version",
          "name",
          "description",
          "ai_context",
          "datasets",
          "relationships",
          "metrics",
          "custom_extensions");
  private static final Set<String> DATASET_PROPERTIES =
      Set.of(
          "name",
          "source",
          "primary_key",
          "unique_keys",
          "description",
          "ai_context",
          "fields",
          "custom_extensions");
  private static final Set<String> FIELD_PROPERTIES =
      Set.of(
          "name",
          "expression",
          "dimension",
          "label",
          "description",
          "datatype",
          "ai_context",
          "custom_extensions");
  private static final Set<String> RELATIONSHIP_PROPERTIES =
      Set.of("name", "from", "to", "from_columns", "to_columns", "ai_context", "custom_extensions");
  private static final Set<String> METRIC_PROPERTIES =
      Set.of("name", "expression", "description", "datatype", "ai_context", "custom_extensions");
  private static final Set<String> EXPRESSION_PROPERTIES = Set.of("dialects");
  private static final Set<String> DIALECT_EXPRESSION_PROPERTIES = Set.of("dialect", "expression");
  private static final Set<String> DIMENSION_PROPERTIES = Set.of("is_time");
  private static final Set<String> CUSTOM_EXTENSION_PROPERTIES = Set.of("vendor_name", "data");

  private static final ObjectMapper JSON_MAPPER = createJsonMapper();
  private static final ObjectMapper YAML_MAPPER = createYamlMapper();

  private OssieSemanticModelDocumentConverter() {}

  /**
   * Converts one standalone Apache Ossie YAML or JSON document into a create request.
   *
   * @param document The standalone Ossie document.
   * @return The converted Semantic Model create request.
   * @throws IllegalSemanticModelException If the document cannot be parsed or represented by
   *     Gravitino.
   */
  public static SemanticModelCreateRequest importDocument(OssieDocument document) {
    Objects.requireNonNull(document, "document must not be null");
    ObjectNode root = parseDocument(document);
    return toCreateRequest(root);
  }

  /**
   * Exports one Gravitino Semantic Model as a standalone Apache Ossie document.
   *
   * @param semanticModel The Semantic Model to export.
   * @param format The requested serialization format.
   * @return The serialized Ossie document.
   * @throws IllegalSemanticModelException If the model cannot be represented as a valid Ossie
   *     document.
   */
  public static OssieDocument exportDocument(SemanticModel semanticModel, OssieFormat format) {
    Objects.requireNonNull(semanticModel, "semanticModel must not be null");
    Objects.requireNonNull(format, "format must not be null");

    ObjectNode definition =
        JSON_MAPPER.valueToTree(
            SemanticModelDefinitionDTO.fromDefinition(semanticModel.definition()));
    transformNativeDefinition(definition);

    ObjectNode root = JSON_MAPPER.createObjectNode();
    root.put("version", ossieVersion(semanticModel.properties()));
    root.put("name", semanticModel.name());
    if (semanticModel.comment() != null) {
      root.put("description", semanticModel.comment());
    }
    definition.fields().forEachRemaining(entry -> root.set(entry.getKey(), entry.getValue()));
    stashProperties(root, semanticModel.properties());

    // Validate the generated document through the same conversion path used for imports.
    toCreateRequest(root.deepCopy());

    try {
      if (format == OssieFormat.JSON) {
        return OssieDocument.json(
            JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n");
      }
      return OssieDocument.yaml(YAML_MAPPER.writeValueAsString(root));
    } catch (JsonProcessingException e) {
      throw new IllegalSemanticModelException(
          e, "Cannot serialize Apache Ossie document: %s", e.getOriginalMessage());
    }
  }

  private static ObjectMapper createJsonMapper() {
    JsonFactory factory =
        JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();
    factory.setStreamReadConstraints(STREAM_READ_CONSTRAINTS);
    return JsonMapper.builder(factory)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
        .build()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  private static ObjectMapper createYamlMapper() {
    YAMLFactory factory =
        YAMLFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .build();
    factory.setStreamReadConstraints(STREAM_READ_CONSTRAINTS);
    return new ObjectMapper(factory)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  private static ObjectNode parseDocument(OssieDocument document) {
    String content = document.content();
    if (StringUtils.isBlank(content)) {
      throw new IllegalSemanticModelException("Apache Ossie document must not be empty");
    }
    if (content.length() > MAX_DOCUMENT_LENGTH) {
      throw new IllegalSemanticModelException(
          "Apache Ossie document exceeds the maximum length of %s characters", MAX_DOCUMENT_LENGTH);
    }

    try {
      ObjectMapper mapper = document.format() == OssieFormat.JSON ? JSON_MAPPER : YAML_MAPPER;
      JsonNode parsed = readDocumentTree(mapper, content);
      if (!(parsed instanceof ObjectNode)) {
        throw invalid("$", "document root must be an object");
      }
      return (ObjectNode) parsed;
    } catch (IOException e) {
      throw new IllegalSemanticModelException(
          e, "Cannot parse Apache Ossie %s: %s", document.format(), originalMessage(e));
    }
  }

  private static JsonNode readDocumentTree(ObjectMapper mapper, String content) throws IOException {
    // Older YAML parsers do not enforce StreamReadConstraints on nesting depth.
    try (JsonParser parser =
        new JsonParserDelegate(mapper.createParser(content)) {
          @Override
          public JsonToken nextToken() throws IOException {
            JsonToken token = super.nextToken();
            STREAM_READ_CONSTRAINTS.validateNestingDepth(getParsingContext().getNestingDepth());
            return token;
          }
        }) {
      // Check the whole document, without enabling this on nested DTO deserializers.
      return mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(parser);
    }
  }

  private static SemanticModelCreateRequest toCreateRequest(ObjectNode root) {
    validateObject(root, "$", ROOT_PROPERTIES);
    validateOptionalText(root, "name", "$");
    validateOptionalText(root, "description", "$");
    String ossieVersion = validateVersion(root.get("version"));

    Map<String, String> properties = new LinkedHashMap<>(extractProperties(root));
    String extensionVersion = properties.get(PROPERTY_OSSIE_VERSION);
    if (extensionVersion != null && !extensionVersion.equals(ossieVersion)) {
      throw invalid(
          "$.custom_extensions",
          "Gravitino property '" + PROPERTY_OSSIE_VERSION + "' conflicts with $.version");
    }
    properties.put(PROPERTY_OSSIE_VERSION, ossieVersion);
    ObjectNode definition = JSON_MAPPER.createObjectNode();
    copy(root, definition, "ai_context");
    copy(root, definition, "datasets");
    copy(root, definition, "relationships");
    copy(root, definition, "metrics");
    copy(root, definition, "custom_extensions");
    transformOssieDefinition(definition, "$");

    ObjectNode requestNode = JSON_MAPPER.createObjectNode();
    copy(root, requestNode, "name");
    if (root.has("description")) {
      requestNode.set("comment", root.get("description"));
    }
    requestNode.set("definition", definition);
    requestNode.set("properties", JSON_MAPPER.valueToTree(properties));

    try {
      SemanticModelCreateRequest request =
          JSON_MAPPER.treeToValue(requestNode, SemanticModelCreateRequest.class);
      request.validate();
      request.toDefinition();
      return request;
    } catch (JsonProcessingException | IllegalArgumentException e) {
      throw new IllegalSemanticModelException(
          e,
          "Cannot convert Apache Ossie document to a Gravitino Semantic Model: %s",
          originalMessage(e));
    }
  }

  private static String validateVersion(@Nullable JsonNode version) {
    if (version == null || !version.isTextual() || StringUtils.isBlank(version.textValue())) {
      throw invalid("$.version", "must be a non-empty string");
    }
    return version.textValue();
  }

  private static String ossieVersion(Map<String, String> properties) {
    String version = properties.getOrDefault(PROPERTY_OSSIE_VERSION, DEFAULT_OSSIE_VERSION);
    if (StringUtils.isBlank(version)) {
      throw invalid(
          "$.version",
          "Semantic Model property '" + PROPERTY_OSSIE_VERSION + "' must not be blank");
    }
    return version;
  }

  private static void transformOssieDefinition(ObjectNode definition, String path) {
    validateAIContext(definition.get("ai_context"), path + ".ai_context");
    transformObjectArray(
        definition.get("datasets"),
        path + ".datasets",
        OssieSemanticModelDocumentConverter::transformOssieDataset);
    transformObjectArray(
        definition.get("relationships"),
        path + ".relationships",
        OssieSemanticModelDocumentConverter::transformOssieRelationship);
    transformObjectArray(
        definition.get("metrics"),
        path + ".metrics",
        OssieSemanticModelDocumentConverter::transformOssieMetric);
    transformOssieCustomExtensions(
        definition.get("custom_extensions"), path + ".custom_extensions");
    rename(definition, "ai_context", "aiContext");
    rename(definition, "custom_extensions", "customExtensions");
  }

  private static void transformOssieDataset(ObjectNode dataset, String path) {
    validateObject(dataset, path, DATASET_PROPERTIES);
    validateOptionalText(dataset, "name", path);
    validateOptionalText(dataset, "description", path);
    validateOptionalStringArray(dataset, "primary_key", path);
    validateOptionalStringArrayArray(dataset, "unique_keys", path);
    JsonNode source = dataset.get("source");
    if (source != null) {
      if (!source.isTextual()) {
        throw invalid(path + ".source", "must be a string");
      }
      String sourceValue = source.textValue();
      String[] parts = parseOssieSource(sourceValue, path + ".source");
      ObjectNode identifier = JSON_MAPPER.createObjectNode();
      identifier.putArray("namespace").add(parts[0]).add(parts[1]);
      identifier.put("name", parts[2]);
      dataset.set("source", identifier);
    }

    validateAIContext(dataset.get("ai_context"), path + ".ai_context");
    transformObjectArray(
        dataset.get("fields"),
        path + ".fields",
        OssieSemanticModelDocumentConverter::transformOssieField);
    transformOssieCustomExtensions(dataset.get("custom_extensions"), path + ".custom_extensions");
    rename(dataset, "primary_key", "primaryKey");
    rename(dataset, "unique_keys", "uniqueKeys");
    rename(dataset, "ai_context", "aiContext");
    rename(dataset, "custom_extensions", "customExtensions");
  }

  private static String[] parseOssieSource(String source, String path) {
    List<String> parts = new ArrayList<>(3);
    int offset = 0;
    while (offset < source.length()) {
      StringBuilder part = new StringBuilder();
      if (source.charAt(offset) == '`') {
        offset++;
        boolean closed = false;
        while (offset < source.length()) {
          char current = source.charAt(offset);
          if (current != '`') {
            part.append(current);
            offset++;
          } else if (offset + 1 < source.length() && source.charAt(offset + 1) == '`') {
            part.append('`');
            offset += 2;
          } else {
            closed = true;
            offset++;
            break;
          }
        }
        if (!closed) {
          throw invalid(path, "contains an unterminated quoted segment");
        }
        if (offset < source.length() && source.charAt(offset) != '.') {
          throw invalid(path, "quoted segments must be separated by dots");
        }
      } else {
        while (offset < source.length() && source.charAt(offset) != '.') {
          char current = source.charAt(offset);
          if (current == '`') {
            throw invalid(path, "backticks must quote an entire segment");
          }
          part.append(current);
          offset++;
        }
      }

      if (StringUtils.isBlank(part)) {
        throw invalid(path, "source segments must not be blank");
      }
      parts.add(part.toString());
      if (offset < source.length()) {
        offset++;
        if (offset == source.length()) {
          throw invalid(path, "source segments must not be blank");
        }
      }
    }

    if (parts.size() != 3) {
      throw invalid(
          path,
          "must be a three-part catalog.schema.entity identifier; query sources are not supported");
    }
    return parts.toArray(new String[0]);
  }

  private static void transformOssieField(ObjectNode field, String path) {
    validateObject(field, path, FIELD_PROPERTIES);
    validateOptionalText(field, "name", path);
    validateOptionalText(field, "label", path);
    validateOptionalText(field, "description", path);
    validateOptionalText(field, "datatype", path);
    transformOssieExpression(field.get("expression"), path + ".expression");
    JsonNode dimension = field.get("dimension");
    if (dimension != null) {
      if (!(dimension instanceof ObjectNode)) {
        throw invalid(path + ".dimension", "must be an object");
      }
      ObjectNode dimensionObject = (ObjectNode) dimension;
      validateObject(dimensionObject, path + ".dimension", DIMENSION_PROPERTIES);
      validateOptionalBoolean(dimensionObject, "is_time", path + ".dimension");
      rename(dimensionObject, "is_time", "isTime");
    }
    validateAIContext(field.get("ai_context"), path + ".ai_context");
    transformOssieCustomExtensions(field.get("custom_extensions"), path + ".custom_extensions");
    rename(field, "ai_context", "aiContext");
    rename(field, "custom_extensions", "customExtensions");
  }

  private static void transformOssieRelationship(ObjectNode relationship, String path) {
    validateObject(relationship, path, RELATIONSHIP_PROPERTIES);
    validateOptionalText(relationship, "name", path);
    validateOptionalText(relationship, "from", path);
    validateOptionalText(relationship, "to", path);
    validateOptionalStringArray(relationship, "from_columns", path);
    validateOptionalStringArray(relationship, "to_columns", path);
    validateAIContext(relationship.get("ai_context"), path + ".ai_context");
    transformOssieCustomExtensions(
        relationship.get("custom_extensions"), path + ".custom_extensions");
    rename(relationship, "from_columns", "fromColumns");
    rename(relationship, "to_columns", "toColumns");
    rename(relationship, "ai_context", "aiContext");
    rename(relationship, "custom_extensions", "customExtensions");
  }

  private static void transformOssieMetric(ObjectNode metric, String path) {
    validateObject(metric, path, METRIC_PROPERTIES);
    validateOptionalText(metric, "name", path);
    validateOptionalText(metric, "description", path);
    validateOptionalText(metric, "datatype", path);
    transformOssieExpression(metric.get("expression"), path + ".expression");
    validateAIContext(metric.get("ai_context"), path + ".ai_context");
    transformOssieCustomExtensions(metric.get("custom_extensions"), path + ".custom_extensions");
    rename(metric, "ai_context", "aiContext");
    rename(metric, "custom_extensions", "customExtensions");
  }

  private static void transformOssieExpression(@Nullable JsonNode expression, String path) {
    if (expression == null) {
      return;
    }
    if (!(expression instanceof ObjectNode)) {
      throw invalid(path, "must be an object");
    }
    ObjectNode expressionObject = (ObjectNode) expression;
    validateObject(expressionObject, path, EXPRESSION_PROPERTIES);
    transformObjectArray(
        expressionObject.get("dialects"),
        path + ".dialects",
        (dialectExpression, dialectPath) -> {
          validateObject(dialectExpression, dialectPath, DIALECT_EXPRESSION_PROPERTIES);
          validateOptionalText(dialectExpression, "dialect", dialectPath);
          validateOptionalText(dialectExpression, "expression", dialectPath);
        });
  }

  private static void transformOssieCustomExtensions(@Nullable JsonNode extensions, String path) {
    transformObjectArray(
        extensions,
        path,
        (extension, extensionPath) -> {
          validateObject(extension, extensionPath, CUSTOM_EXTENSION_PROPERTIES);
          validateOptionalText(extension, "vendor_name", extensionPath);
          validateOptionalText(extension, "data", extensionPath);
          rename(extension, "vendor_name", "vendorName");
        });
  }

  private static void validateAIContext(@Nullable JsonNode context, String path) {
    if (context == null) {
      return;
    }
    if (context.isTextual()) {
      return;
    }
    if (!(context instanceof ObjectNode)) {
      throw invalid(path, "must be a string or object");
    }

    ObjectNode object = (ObjectNode) context;
    validateOptionalText(object, "instructions", path);
    validateOptionalStringArray(object, "synonyms", path);
    validateOptionalStringArray(object, "examples", path);
  }

  private static void transformNativeDefinition(ObjectNode definition) {
    transformObjectArray(
        definition.get("datasets"),
        "$.datasets",
        OssieSemanticModelDocumentConverter::transformNativeDataset);
    transformObjectArray(
        definition.get("relationships"),
        "$.relationships",
        OssieSemanticModelDocumentConverter::transformNativeRelationship);
    transformObjectArray(
        definition.get("metrics"),
        "$.metrics",
        OssieSemanticModelDocumentConverter::transformNativeMetric);
    transformNativeCustomExtensions(definition.get("customExtensions"), "$.custom_extensions");
    rename(definition, "aiContext", "ai_context");
    rename(definition, "customExtensions", "custom_extensions");
  }

  private static void transformNativeDataset(ObjectNode dataset, String path) {
    JsonNode source = dataset.get("source");
    if (!(source instanceof ObjectNode)) {
      throw invalid(path + ".source", "must be a Gravitino source identifier");
    }
    JsonNode namespace = source.get("namespace");
    JsonNode name = source.get("name");
    if (!(namespace instanceof ArrayNode)
        || namespace.size() != 2
        || !namespace.get(0).isTextual()
        || !namespace.get(1).isTextual()
        || name == null
        || !name.isTextual()) {
      throw invalid(path + ".source", "must contain exactly catalog.schema.name");
    }
    dataset.put(
        "source",
        formatOssieSourceSegment(namespace.get(0).textValue())
            + "."
            + formatOssieSourceSegment(namespace.get(1).textValue())
            + "."
            + formatOssieSourceSegment(name.textValue()));

    transformObjectArray(
        dataset.get("fields"),
        path + ".fields",
        OssieSemanticModelDocumentConverter::transformNativeField);
    transformNativeCustomExtensions(dataset.get("customExtensions"), path + ".custom_extensions");
    rename(dataset, "primaryKey", "primary_key");
    rename(dataset, "uniqueKeys", "unique_keys");
    rename(dataset, "aiContext", "ai_context");
    rename(dataset, "customExtensions", "custom_extensions");
  }

  private static String formatOssieSourceSegment(String segment) {
    if (!segment.contains(".") && !segment.contains("`")) {
      return segment;
    }
    return "`" + segment.replace("`", "``") + "`";
  }

  private static void transformNativeField(ObjectNode field, String path) {
    JsonNode dimension = field.get("dimension");
    if (dimension instanceof ObjectNode) {
      rename((ObjectNode) dimension, "isTime", "is_time");
    }
    transformNativeCustomExtensions(field.get("customExtensions"), path + ".custom_extensions");
    rename(field, "aiContext", "ai_context");
    rename(field, "customExtensions", "custom_extensions");
  }

  private static void transformNativeRelationship(ObjectNode relationship, String path) {
    transformNativeCustomExtensions(
        relationship.get("customExtensions"), path + ".custom_extensions");
    rename(relationship, "fromColumns", "from_columns");
    rename(relationship, "toColumns", "to_columns");
    rename(relationship, "aiContext", "ai_context");
    rename(relationship, "customExtensions", "custom_extensions");
  }

  private static void transformNativeMetric(ObjectNode metric, String path) {
    transformNativeCustomExtensions(metric.get("customExtensions"), path + ".custom_extensions");
    rename(metric, "aiContext", "ai_context");
    rename(metric, "customExtensions", "custom_extensions");
  }

  private static void transformNativeCustomExtensions(@Nullable JsonNode extensions, String path) {
    transformObjectArray(
        extensions,
        path,
        (extension, extensionPath) -> rename(extension, "vendorName", "vendor_name"));
  }

  private static void stashProperties(ObjectNode root, Map<String, String> properties) {
    Map<String, String> interchangeProperties = new LinkedHashMap<>();
    if (properties != null) {
      properties.forEach(
          (key, value) -> {
            if (!PROPERTY_OSSIE_VERSION.equals(key)) {
              interchangeProperties.put(key, value);
            }
          });
    }

    JsonNode existingExtensions = root.get("custom_extensions");
    ArrayNode extensions;
    if (existingExtensions == null) {
      if (interchangeProperties.isEmpty()) {
        return;
      }
      extensions = root.putArray("custom_extensions");
    } else if (existingExtensions instanceof ArrayNode) {
      extensions = (ArrayNode) existingExtensions;
    } else {
      throw invalid("$.custom_extensions", "must be an array");
    }
    ensureNoReservedMarker(extensions);

    if (interchangeProperties.isEmpty()) {
      return;
    }

    ObjectNode payload = JSON_MAPPER.createObjectNode();
    ObjectNode marker = payload.putObject(INTERCHANGE_MARKER);
    marker.put("version", INTERCHANGE_MARKER_VERSION);
    marker.set("properties", JSON_MAPPER.valueToTree(interchangeProperties));

    ObjectNode extension = extensions.addObject();
    extension.put("vendor_name", GRAVITINO_VENDOR);
    extension.put("data", writeExtensionData(payload));
  }

  private static Map<String, String> extractProperties(ObjectNode root) {
    JsonNode extensionsNode = root.get("custom_extensions");
    if (!(extensionsNode instanceof ArrayNode)) {
      return Map.of();
    }

    ArrayNode extensions = (ArrayNode) extensionsNode;
    Map<String, String> properties = new LinkedHashMap<>();
    boolean found = false;
    Iterator<JsonNode> iterator = extensions.iterator();
    while (iterator.hasNext()) {
      JsonNode candidate = iterator.next();
      if (!(candidate instanceof ObjectNode)) {
        continue;
      }
      ObjectNode extension = (ObjectNode) candidate;
      if (!GRAVITINO_VENDOR.equals(extension.path("vendor_name").asText(null))) {
        continue;
      }

      ObjectNode payload = parseExtensionData(extension.path("data").asText(null));
      JsonNode markerNode = payload == null ? null : payload.get(INTERCHANGE_MARKER);
      if (markerNode == null) {
        continue;
      }
      if (found) {
        throw invalid("$.custom_extensions", "contains multiple Gravitino interchange markers");
      }
      if (!(markerNode instanceof ObjectNode)
          || markerNode.path("version").asInt(-1) != INTERCHANGE_MARKER_VERSION
          || !(markerNode.get("properties") instanceof ObjectNode)) {
        throw invalid(
            "$.custom_extensions", "contains an unsupported Gravitino interchange marker");
      }

      ObjectNode propertyNode = (ObjectNode) markerNode.get("properties");
      propertyNode
          .fields()
          .forEachRemaining(
              entry -> {
                if (!entry.getValue().isTextual()) {
                  throw invalid(
                      "$.custom_extensions",
                      "Gravitino property '" + entry.getKey() + "' must be a string");
                }
                properties.put(entry.getKey(), entry.getValue().textValue());
              });
      found = true;

      payload.remove(INTERCHANGE_MARKER);
      if (payload.isEmpty()) {
        iterator.remove();
      } else {
        extension.put("data", writeExtensionData(payload));
      }
    }
    if (extensions.isEmpty()) {
      root.remove("custom_extensions");
    }
    return properties;
  }

  private static void ensureNoReservedMarker(ArrayNode extensions) {
    for (JsonNode candidate : extensions) {
      if (!(candidate instanceof ObjectNode)
          || !GRAVITINO_VENDOR.equals(candidate.path("vendor_name").asText(null))) {
        continue;
      }
      ObjectNode payload = parseExtensionData(candidate.path("data").asText(null));
      if (payload != null && payload.has(INTERCHANGE_MARKER)) {
        throw invalid(
            "$.custom_extensions", "already contains a reserved Gravitino interchange marker");
      }
    }
  }

  @Nullable
  private static ObjectNode parseExtensionData(@Nullable String data) {
    if (data == null) {
      return null;
    }
    try {
      JsonNode parsed =
          JSON_MAPPER.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(data);
      return parsed instanceof ObjectNode ? (ObjectNode) parsed : null;
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  private static String writeExtensionData(ObjectNode payload) {
    try {
      return JSON_MAPPER.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalSemanticModelException(
          e, "Cannot serialize Gravitino interchange extension: %s", e.getOriginalMessage());
    }
  }

  private static void validateObject(ObjectNode object, String path, Set<String> allowed) {
    object
        .fields()
        .forEachRemaining(
            entry -> {
              if (!allowed.contains(entry.getKey())) {
                throw invalid(
                    path + "." + entry.getKey(), "property is not defined by Apache Ossie");
              }
              if (entry.getValue().isNull()) {
                throw invalid(path + "." + entry.getKey(), "must not be null");
              }
            });
  }

  private static void validateOptionalText(ObjectNode object, String name, String path) {
    JsonNode value = object.get(name);
    if (value != null && !value.isTextual()) {
      throw invalid(path + "." + name, "must be a string");
    }
  }

  private static void validateOptionalBoolean(ObjectNode object, String name, String path) {
    JsonNode value = object.get(name);
    if (value != null && !value.isBoolean()) {
      throw invalid(path + "." + name, "must be a boolean");
    }
  }

  private static void validateOptionalStringArray(ObjectNode object, String name, String path) {
    JsonNode value = object.get(name);
    if (value == null) {
      return;
    }
    if (!(value instanceof ArrayNode)) {
      throw invalid(path + "." + name, "must be an array of strings");
    }
    for (int index = 0; index < value.size(); index++) {
      if (!value.get(index).isTextual()) {
        throw invalid(path + "." + name + "[" + index + "]", "must be a string");
      }
    }
  }

  private static void validateOptionalStringArrayArray(
      ObjectNode object, String name, String path) {
    JsonNode value = object.get(name);
    if (value == null) {
      return;
    }
    if (!(value instanceof ArrayNode)) {
      throw invalid(path + "." + name, "must be an array of string arrays");
    }
    for (int outerIndex = 0; outerIndex < value.size(); outerIndex++) {
      JsonNode nestedValue = value.get(outerIndex);
      if (!(nestedValue instanceof ArrayNode)) {
        throw invalid(path + "." + name + "[" + outerIndex + "]", "must be an array of strings");
      }
      for (int innerIndex = 0; innerIndex < nestedValue.size(); innerIndex++) {
        if (!nestedValue.get(innerIndex).isTextual()) {
          throw invalid(
              path + "." + name + "[" + outerIndex + "][" + innerIndex + "]", "must be a string");
        }
      }
    }
  }

  private static void transformObjectArray(
      @Nullable JsonNode node, String path, ObjectTransformer transformer) {
    if (node == null) {
      return;
    }
    if (!(node instanceof ArrayNode)) {
      throw invalid(path, "must be an array of objects");
    }
    ArrayNode array = (ArrayNode) node;
    for (int index = 0; index < array.size(); index++) {
      JsonNode item = array.get(index);
      if (!(item instanceof ObjectNode)) {
        throw invalid(path + "[" + index + "]", "must be an object");
      }
      transformer.transform((ObjectNode) item, path + "[" + index + "]");
    }
  }

  private static void copy(ObjectNode source, ObjectNode target, String name) {
    JsonNode value = source.get(name);
    if (value != null) {
      target.set(name, value.deepCopy());
    }
  }

  private static void rename(ObjectNode object, String source, String target) {
    JsonNode value = object.remove(source);
    if (value != null) {
      object.set(target, value);
    }
  }

  private static String originalMessage(Exception exception) {
    if (exception instanceof JsonProcessingException) {
      return ((JsonProcessingException) exception).getOriginalMessage();
    }
    return exception.getMessage();
  }

  private static IllegalSemanticModelException invalid(String path, String detail) {
    return new IllegalSemanticModelException("%s: %s", path, detail);
  }

  @FunctionalInterface
  private interface ObjectTransformer {
    void transform(ObjectNode object, String path);
  }
}
