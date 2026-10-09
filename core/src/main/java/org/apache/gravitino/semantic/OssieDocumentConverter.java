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

import static org.apache.gravitino.semantic.CustomExtension.GRAVITINO_PROPERTIES_VENDOR;
import static org.apache.gravitino.semantic.OssieVersion.DEFAULT_VERSION;
import static org.apache.gravitino.semantic.SemanticModel.PROPERTY_OSSIE_VERSION;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;

/** Converts between standalone Apache Ossie documents and Gravitino Semantic Models. */
public final class OssieDocumentConverter {

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

  private static final Set<String> AI_CONTEXT_PROPERTIES =
      Set.of("instructions", "synonyms", "examples");
  private static final Map<DataType, String> DATA_TYPE_NAMES =
      Map.of(
          DataType.STRING, "String",
          DataType.INTEGER, "Integer",
          DataType.DECIMAL, "Decimal",
          DataType.FLOAT, "Float",
          DataType.BOOLEAN, "Boolean",
          DataType.DATE, "Date",
          DataType.TIME, "Time",
          DataType.DATE_TIME, "DateTime",
          DataType.DATE_TIME_TZ, "DateTimeTz",
          DataType.OPAQUE, "Opaque");

  private static final ObjectMapper JSON_MAPPER = createJsonMapper();
  private static final ObjectMapper YAML_MAPPER = createYamlMapper();

  private OssieDocumentConverter() {}

  /**
   * Converts one standalone Apache Ossie YAML or JSON document into native model values.
   *
   * @param document The standalone Ossie document.
   * @return The model name, comment, native definition, and properties.
   * @throws IllegalSemanticModelException If the document cannot be parsed or represented by
   *     Gravitino.
   */
  public static ImportedSemanticModel importDocument(OssieDocument document) {
    Objects.requireNonNull(document, "document must not be null");
    ObjectNode root = parseDocument(document);
    return toImportedModel(root);
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

    ObjectNode definition = writeDefinition(semanticModel.definition());

    ObjectNode root = JSON_MAPPER.createObjectNode();
    root.put("version", ossieVersion(semanticModel.properties()));
    root.put("name", semanticModel.name());
    if (semanticModel.comment() != null) {
      root.put("description", semanticModel.comment());
    }
    definition.fields().forEachRemaining(entry -> root.set(entry.getKey(), entry.getValue()));
    stashProperties(root, semanticModel.properties());

    // Validate the generated document through the same conversion path used for imports.
    toImportedModel(root.deepCopy());

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

  /** Native model values parsed from an Ossie document, before the model is created. */
  public static final class ImportedSemanticModel {

    private final String name;
    @Nullable private final String comment;
    private final SemanticModelDefinition definition;
    private final Map<String, String> properties;

    private ImportedSemanticModel(
        String name,
        @Nullable String comment,
        SemanticModelDefinition definition,
        Map<String, String> properties) {
      this.name = name;
      this.comment = comment;
      this.definition = definition;
      this.properties = Map.copyOf(properties);
    }

    /**
     * Returns the model name supplied by the document.
     *
     * @return The model name.
     */
    public String name() {
      return name;
    }

    /**
     * Returns the document description.
     *
     * @return The model comment, or {@code null} when absent.
     */
    @Nullable
    public String comment() {
      return comment;
    }

    /**
     * Returns the parsed native definition.
     *
     * @return The immutable Semantic Model definition.
     */
    public SemanticModelDefinition definition() {
      return definition;
    }

    /**
     * Returns the Gravitino properties, including the Ossie version.
     *
     * @return The immutable properties.
     */
    public Map<String, String> properties() {
      return properties;
    }
  }

  private static ObjectMapper createJsonMapper() {
    JsonFactory factory =
        JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();
    return JsonMapper.builder(factory)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
        .build();
  }

  private static ObjectMapper createYamlMapper() {
    YAMLFactory factory =
        YAMLFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .build();
    return new ObjectMapper(factory)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);
  }

  private static ObjectNode parseDocument(OssieDocument document) {
    String content = document.content();
    try {
      ObjectMapper mapper = document.format() == OssieFormat.JSON ? JSON_MAPPER : YAML_MAPPER;
      // Reject trailing content after the standalone document.
      JsonNode parsed =
          mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(content);
      if (!(parsed instanceof ObjectNode)) {
        throw invalid("$", "document root must be an object");
      }
      return (ObjectNode) parsed;
    } catch (IOException e) {
      throw new IllegalSemanticModelException(
          e, "Cannot parse Apache Ossie %s: %s", document.format(), originalMessage(e));
    }
  }

  private static ImportedSemanticModel toImportedModel(ObjectNode root) {
    validateObject(root, "$", ROOT_PROPERTIES);
    String name = readText(root, "name", "$");
    String comment = readText(root, "description", "$");
    String ossieVersion = validateVersion(root.get("version"));

    Map<String, String> properties = new LinkedHashMap<>(extractProperties(root));
    String extensionVersion = properties.get(PROPERTY_OSSIE_VERSION);
    if (extensionVersion != null && !extensionVersion.equals(ossieVersion)) {
      throw invalid(
          "$.custom_extensions",
          "Gravitino property '" + PROPERTY_OSSIE_VERSION + "' conflicts with $.version");
    }
    properties.put(PROPERTY_OSSIE_VERSION, ossieVersion);

    try {
      if (StringUtils.isBlank(name)) {
        throw new IllegalArgumentException("\"name\" field is required and cannot be empty");
      }
      return new ImportedSemanticModel(name, comment, readDefinition(root), properties);
    } catch (IllegalArgumentException e) {
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
    String version = properties.getOrDefault(PROPERTY_OSSIE_VERSION, DEFAULT_VERSION);
    if (StringUtils.isBlank(version)) {
      throw invalid(
          "$.version",
          "Semantic Model property '" + PROPERTY_OSSIE_VERSION + "' must not be blank");
    }
    return version;
  }

  private static SemanticModelDefinition readDefinition(ObjectNode root) {
    return SemanticModelDefinition.builder()
        .withAIContext(readAIContext(root.get("ai_context"), "$.ai_context"))
        .withDatasets(
            readObjectArray(
                root.get("datasets"),
                "$.datasets",
                OssieDocumentConverter::readDataset,
                Dataset[]::new))
        .withRelationships(
            readObjectArray(
                root.get("relationships"),
                "$.relationships",
                OssieDocumentConverter::readRelationship,
                Relationship[]::new))
        .withMetrics(
            readObjectArray(
                root.get("metrics"),
                "$.metrics",
                OssieDocumentConverter::readMetric,
                Metric[]::new))
        .withCustomExtensions(
            readCustomExtensions(root.get("custom_extensions"), "$.custom_extensions"))
        .build();
  }

  private static Dataset readDataset(ObjectNode dataset, String path) {
    validateObject(dataset, path, DATASET_PROPERTIES);
    String source = readText(dataset, "source", path);
    return Dataset.builder()
        .withName(readText(dataset, "name", path))
        .withSource(
            source == null ? null : NameIdentifier.of(parseOssieSource(source, path + ".source")))
        .withPrimaryKey(readStringArray(dataset, "primary_key", path))
        .withUniqueKeys(readStringArrays(dataset, "unique_keys", path))
        .withDescription(readText(dataset, "description", path))
        .withAIContext(readAIContext(dataset.get("ai_context"), path + ".ai_context"))
        .withFields(
            readObjectArray(
                dataset.get("fields"),
                path + ".fields",
                OssieDocumentConverter::readField,
                Field[]::new))
        .withCustomExtensions(
            readCustomExtensions(dataset.get("custom_extensions"), path + ".custom_extensions"))
        .build();
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

  private static Field readField(ObjectNode field, String path) {
    validateObject(field, path, FIELD_PROPERTIES);
    return Field.builder()
        .withName(readText(field, "name", path))
        .withExpression(readExpression(field.get("expression"), path + ".expression"))
        .withDimension(readDimension(field.get("dimension"), path + ".dimension"))
        .withLabel(readText(field, "label", path))
        .withDescription(readText(field, "description", path))
        .withDatatype(readDataType(field, path))
        .withAIContext(readAIContext(field.get("ai_context"), path + ".ai_context"))
        .withCustomExtensions(
            readCustomExtensions(field.get("custom_extensions"), path + ".custom_extensions"))
        .build();
  }

  private static Relationship readRelationship(ObjectNode relationship, String path) {
    validateObject(relationship, path, RELATIONSHIP_PROPERTIES);
    return Relationship.builder()
        .withName(readText(relationship, "name", path))
        .withFrom(readText(relationship, "from", path))
        .withTo(readText(relationship, "to", path))
        .withFromColumns(readStringArray(relationship, "from_columns", path))
        .withToColumns(readStringArray(relationship, "to_columns", path))
        .withAIContext(readAIContext(relationship.get("ai_context"), path + ".ai_context"))
        .withCustomExtensions(
            readCustomExtensions(
                relationship.get("custom_extensions"), path + ".custom_extensions"))
        .build();
  }

  private static Metric readMetric(ObjectNode metric, String path) {
    validateObject(metric, path, METRIC_PROPERTIES);
    return Metric.builder()
        .withName(readText(metric, "name", path))
        .withExpression(readExpression(metric.get("expression"), path + ".expression"))
        .withDescription(readText(metric, "description", path))
        .withDatatype(readDataType(metric, path))
        .withAIContext(readAIContext(metric.get("ai_context"), path + ".ai_context"))
        .withCustomExtensions(
            readCustomExtensions(metric.get("custom_extensions"), path + ".custom_extensions"))
        .build();
  }

  @Nullable
  private static Expression readExpression(@Nullable JsonNode node, String path) {
    if (node == null) {
      return null;
    }
    ObjectNode expression = requireObject(node, path);
    validateObject(expression, path, EXPRESSION_PROPERTIES);
    return Expression.builder()
        .withDialects(
            readObjectArray(
                expression.get("dialects"),
                path + ".dialects",
                OssieDocumentConverter::readDialectExpression,
                DialectExpression[]::new))
        .build();
  }

  private static DialectExpression readDialectExpression(ObjectNode dialect, String path) {
    validateObject(dialect, path, DIALECT_EXPRESSION_PROPERTIES);
    return DialectExpression.builder()
        .withDialect(readText(dialect, "dialect", path))
        .withExpression(readText(dialect, "expression", path))
        .build();
  }

  @Nullable
  private static Dimension readDimension(@Nullable JsonNode node, String path) {
    if (node == null) {
      return null;
    }
    ObjectNode dimension = requireObject(node, path);
    validateObject(dimension, path, DIMENSION_PROPERTIES);
    validateOptionalBoolean(dimension, "is_time", path);
    JsonNode isTime = dimension.get("is_time");
    return Dimension.builder().withIsTime(isTime == null ? null : isTime.booleanValue()).build();
  }

  @Nullable
  private static DataType readDataType(ObjectNode object, String path) {
    String value = readText(object, "datatype", path);
    if (value == null) {
      return null;
    }
    for (DataType type : DataType.values()) {
      if (value.equals(DATA_TYPE_NAMES.get(type))) {
        return type;
      }
    }
    throw invalid(
        path + ".datatype",
        "Unknown Semantic Model data type: "
            + value
            + ". Supported values: "
            + Arrays.stream(DataType.values())
                .map(DATA_TYPE_NAMES::get)
                .collect(Collectors.joining(", ")));
  }

  @Nullable
  private static CustomExtension[] readCustomExtensions(@Nullable JsonNode node, String path) {
    return readObjectArray(
        node,
        path,
        (extension, extensionPath) -> {
          validateObject(extension, extensionPath, CUSTOM_EXTENSION_PROPERTIES);
          return CustomExtension.builder()
              .withVendorName(readText(extension, "vendor_name", extensionPath))
              .withData(readText(extension, "data", extensionPath))
              .build();
        },
        CustomExtension[]::new);
  }

  @Nullable
  private static AIContext readAIContext(@Nullable JsonNode node, String path) {
    if (node == null) {
      return null;
    }
    if (node.isTextual()) {
      return AIContext.of(node.textValue());
    }
    if (!(node instanceof ObjectNode)) {
      throw invalid(path, "must be a string or object");
    }
    ObjectNode object = (ObjectNode) node;
    Map<String, Object> additionalProperties = new LinkedHashMap<>();
    object
        .fields()
        .forEachRemaining(
            entry -> {
              if (!AI_CONTEXT_PROPERTIES.contains(entry.getKey())) {
                additionalProperties.put(
                    entry.getKey(), JSON_MAPPER.convertValue(entry.getValue(), Object.class));
              }
            });
    return AIContext.of(
        AIContextObject.builder()
            .withInstructions(readText(object, "instructions", path))
            .withSynonyms(readStringArray(object, "synonyms", path))
            .withExamples(readStringArray(object, "examples", path))
            .withAdditionalProperties(additionalProperties)
            .build());
  }

  private static ObjectNode writeDefinition(SemanticModelDefinition definition) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    putOptional(node, "ai_context", writeAIContext(definition.aiContext()));
    writeObjectArray(node, "datasets", definition.datasets(), OssieDocumentConverter::writeDataset);
    writeObjectArray(
        node,
        "relationships",
        definition.relationships(),
        OssieDocumentConverter::writeRelationship);
    writeObjectArray(node, "metrics", definition.metrics(), OssieDocumentConverter::writeMetric);
    writeObjectArray(
        node,
        "custom_extensions",
        definition.customExtensions(),
        OssieDocumentConverter::writeCustomExtension);
    return node;
  }

  private static ObjectNode writeDataset(Dataset dataset) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    node.put("name", dataset.name());
    NameIdentifier source = dataset.source();
    String[] namespace = source.namespace().levels();
    if (namespace.length != 2) {
      throw invalid(
          "$.datasets." + dataset.name() + ".source", "must contain exactly catalog.schema.name");
    }
    node.put(
        "source",
        formatOssieSourceSegment(namespace[0])
            + "."
            + formatOssieSourceSegment(namespace[1])
            + "."
            + formatOssieSourceSegment(source.name()));
    putOptional(node, "primary_key", dataset.primaryKey());
    putOptional(node, "unique_keys", dataset.uniqueKeys());
    putOptional(node, "description", dataset.description());
    putOptional(node, "ai_context", writeAIContext(dataset.aiContext()));
    writeObjectArray(node, "fields", dataset.fields(), OssieDocumentConverter::writeField);
    writeObjectArray(
        node,
        "custom_extensions",
        dataset.customExtensions(),
        OssieDocumentConverter::writeCustomExtension);
    return node;
  }

  private static String formatOssieSourceSegment(String segment) {
    if (!segment.contains(".") && !segment.contains("`")) {
      return segment;
    }
    return "`" + segment.replace("`", "``") + "`";
  }

  private static ObjectNode writeField(Field field) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    node.put("name", field.name());
    node.set("expression", writeExpression(field.expression()));
    if (field.dimension() != null) {
      ObjectNode dimension = node.putObject("dimension");
      putOptional(dimension, "is_time", field.dimension().isTime());
    }
    putOptional(node, "label", field.label());
    putOptional(node, "description", field.description());
    putOptional(
        node, "datatype", field.datatype() == null ? null : DATA_TYPE_NAMES.get(field.datatype()));
    putOptional(node, "ai_context", writeAIContext(field.aiContext()));
    writeObjectArray(
        node,
        "custom_extensions",
        field.customExtensions(),
        OssieDocumentConverter::writeCustomExtension);
    return node;
  }

  private static ObjectNode writeRelationship(Relationship relationship) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    node.put("name", relationship.name());
    node.put("from", relationship.from());
    node.put("to", relationship.to());
    putOptional(node, "from_columns", relationship.fromColumns());
    putOptional(node, "to_columns", relationship.toColumns());
    putOptional(node, "ai_context", writeAIContext(relationship.aiContext()));
    writeObjectArray(
        node,
        "custom_extensions",
        relationship.customExtensions(),
        OssieDocumentConverter::writeCustomExtension);
    return node;
  }

  private static ObjectNode writeMetric(Metric metric) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    node.put("name", metric.name());
    node.set("expression", writeExpression(metric.expression()));
    putOptional(node, "description", metric.description());
    putOptional(
        node,
        "datatype",
        metric.datatype() == null ? null : DATA_TYPE_NAMES.get(metric.datatype()));
    putOptional(node, "ai_context", writeAIContext(metric.aiContext()));
    writeObjectArray(
        node,
        "custom_extensions",
        metric.customExtensions(),
        OssieDocumentConverter::writeCustomExtension);
    return node;
  }

  private static ObjectNode writeExpression(Expression expression) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    writeObjectArray(
        node,
        "dialects",
        expression.dialects(),
        dialect -> {
          ObjectNode value = JSON_MAPPER.createObjectNode();
          value.put("dialect", dialect.dialect());
          value.put("expression", dialect.expression());
          return value;
        });
    return node;
  }

  private static ObjectNode writeCustomExtension(CustomExtension extension) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    node.put("vendor_name", extension.vendorName());
    node.put("data", extension.data());
    return node;
  }

  @Nullable
  private static JsonNode writeAIContext(@Nullable AIContext context) {
    if (context == null) {
      return null;
    }
    if (context.isText()) {
      return JSON_MAPPER.getNodeFactory().textNode(context.text());
    }
    AIContextObject object = context.object();
    ObjectNode node = JSON_MAPPER.createObjectNode();
    putOptional(node, "instructions", object.instructions());
    putOptional(node, "synonyms", object.synonyms());
    putOptional(node, "examples", object.examples());
    object
        .additionalProperties()
        .forEach((key, value) -> node.set(key, JSON_MAPPER.valueToTree(value)));
    return node;
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
    ensureNoPropertiesExtension(extensions);

    if (interchangeProperties.isEmpty()) {
      return;
    }

    ObjectNode extension = extensions.addObject();
    extension.put("vendor_name", GRAVITINO_PROPERTIES_VENDOR);
    extension.put("data", writeExtensionData(JSON_MAPPER.valueToTree(interchangeProperties)));
  }

  private static Map<String, String> extractProperties(ObjectNode root) {
    JsonNode extensionsNode = root.get("custom_extensions");
    if (!(extensionsNode instanceof ArrayNode)) {
      return Map.of();
    }

    ArrayNode extensions = (ArrayNode) extensionsNode;
    Map<String, String> properties = new LinkedHashMap<>();
    boolean found = false;
    int index = 0;
    Iterator<JsonNode> iterator = extensions.iterator();
    while (iterator.hasNext()) {
      JsonNode candidate = iterator.next();
      String path = "$.custom_extensions[" + index++ + "]";
      if (!(candidate instanceof ObjectNode)) {
        continue;
      }
      ObjectNode extension = (ObjectNode) candidate;
      if (!GRAVITINO_PROPERTIES_VENDOR.equals(extension.path("vendor_name").asText(null))) {
        continue;
      }

      if (found) {
        throw invalid(
            "$.custom_extensions",
            "contains multiple " + GRAVITINO_PROPERTIES_VENDOR + " extensions");
      }

      validateObject(extension, path, CUSTOM_EXTENSION_PROPERTIES);
      ObjectNode propertyNode = parsePropertiesData(extension.get("data"), path + ".data");
      propertyNode
          .fields()
          .forEachRemaining(
              entry -> {
                if (!entry.getValue().isTextual()) {
                  throw invalid(
                      path + ".data",
                      "Gravitino property '" + entry.getKey() + "' must be a string");
                }
                properties.put(entry.getKey(), entry.getValue().textValue());
              });
      found = true;

      iterator.remove();
    }
    if (extensions.isEmpty()) {
      root.remove("custom_extensions");
    }
    return properties;
  }

  private static void ensureNoPropertiesExtension(ArrayNode extensions) {
    for (int index = 0; index < extensions.size(); index++) {
      if (GRAVITINO_PROPERTIES_VENDOR.equals(
          extensions.get(index).path("vendor_name").asText(null))) {
        throw invalid(
            "$.custom_extensions[" + index + "].vendor_name",
            "'" + GRAVITINO_PROPERTIES_VENDOR + "' is reserved for Gravitino properties");
      }
    }
  }

  private static ObjectNode parsePropertiesData(@Nullable JsonNode data, String path) {
    if (data == null || !data.isTextual()) {
      throw invalid(path, "must be a JSON string containing an object with string values");
    }
    try {
      JsonNode parsed =
          JSON_MAPPER
              .reader()
              .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
              .readTree(data.textValue());
      if (!(parsed instanceof ObjectNode)) {
        throw invalid(path, "must contain a JSON object with string values");
      }
      return (ObjectNode) parsed;
    } catch (JsonProcessingException e) {
      throw invalid(path, "cannot parse Gravitino properties: " + e.getOriginalMessage());
    }
  }

  private static String writeExtensionData(ObjectNode payload) {
    try {
      return JSON_MAPPER.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalSemanticModelException(
          e, "Cannot serialize Gravitino properties extension: %s", e.getOriginalMessage());
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

  @Nullable
  private static String readText(ObjectNode object, String name, String path) {
    validateOptionalText(object, name, path);
    JsonNode value = object.get(name);
    return value == null ? null : value.textValue();
  }

  @Nullable
  private static String[] readStringArray(ObjectNode object, String name, String path) {
    validateOptionalStringArray(object, name, path);
    return object.has(name) ? stringArray(object.get(name)) : null;
  }

  private static String[] stringArray(JsonNode array) {
    String[] values = new String[array.size()];
    for (int index = 0; index < values.length; index++) {
      values[index] = array.get(index).textValue();
    }
    return values;
  }

  @Nullable
  private static String[][] readStringArrays(ObjectNode object, String name, String path) {
    validateOptionalStringArrayArray(object, name, path);
    JsonNode array = object.get(name);
    if (array == null) {
      return null;
    }
    String[][] values = new String[array.size()][];
    for (int index = 0; index < values.length; index++) {
      values[index] = stringArray(array.get(index));
    }
    return values;
  }

  private static ObjectNode requireObject(JsonNode node, String path) {
    if (!(node instanceof ObjectNode)) {
      throw invalid(path, "must be an object");
    }
    return (ObjectNode) node;
  }

  @Nullable
  private static <T> T[] readObjectArray(
      @Nullable JsonNode node, String path, ObjectReader<T> reader, IntFunction<T[]> arrayFactory) {
    if (node == null) {
      return null;
    }
    if (!(node instanceof ArrayNode)) {
      throw invalid(path, "must be an array of objects");
    }
    T[] values = arrayFactory.apply(node.size());
    for (int index = 0; index < values.length; index++) {
      String itemPath = path + "[" + index + "]";
      values[index] = reader.read(requireObject(node.get(index), itemPath), itemPath);
    }
    return values;
  }

  private static <T> void writeObjectArray(
      ObjectNode object, String name, @Nullable T[] values, Function<T, ObjectNode> writer) {
    if (values != null) {
      ArrayNode array = object.putArray(name);
      for (T value : values) {
        array.add(writer.apply(value));
      }
    }
  }

  private static void putOptional(ObjectNode object, String name, @Nullable Object value) {
    if (value != null) {
      object.set(name, JSON_MAPPER.valueToTree(value));
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
  private interface ObjectReader<T> {
    T read(ObjectNode object, String path);
  }
}
