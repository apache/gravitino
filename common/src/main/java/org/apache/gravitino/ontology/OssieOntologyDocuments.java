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
package org.apache.gravitino.ontology;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.PathType;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;

/**
 * Converts standalone Ossie ontology documents using the bundled schema pin.
 *
 * <p>The schemas under {@code ossie/} are copied from <a
 * href="https://github.com/apache/ossie/tree/3719487ff2744b207d0cc9bed66f5406f410dc3d">Apache Ossie
 * at 3719487</a>, and every schema reference resolves against these bundled resources. Gravitino
 * accepts any nonempty document version, so the root {@code version} constraint in {@code
 * ontology/ontology.json} uses {@code minLength: 1}. Re-apply that change when the schema is
 * updated from upstream; {@code TestOssieOntologyDocuments#testAnyNonBlankVersionIsAccepted} fails
 * otherwise.
 */
public final class OssieOntologyDocuments {

  /** The Ossie version for new native ontologies. */
  public static final String DEFAULT_VERSION = "0.3.0.dev0";

  /** The property that carries the version written to an Ossie document. */
  public static final String VERSION_PROPERTY = "ossie-version";

  private static final String ANNOTATIONS = "annotations";
  private static final String ONTOLOGY_PROPERTIES = "gravitino_properties";
  private static final TypeReference<Map<String, Object>> FIELDS =
      new TypeReference<Map<String, Object>>() {};
  private static final Set<String> OPAQUE_FIELDS =
      ImmutableSet.of(ANNOTATIONS, "prefixes", "ai_context", "aiContext");
  private static final ImmutableBiMap<String, String> OSSIE_TO_NATIVE =
      ImmutableBiMap.<String, String>builder()
          .put("ai_context", "aiContext")
          .put("identify_by", "identifyBy")
          .put("derived_by", "derivedBy")
          .build();

  private static final ObjectMapper JSON =
      configure(
          new ObjectMapper(
              JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build()));
  private static final ObjectMapper YAML =
      configure(
          new ObjectMapper(
              YAMLFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                  .build()));

  private static final JsonSchemaFactory SCHEMAS =
      JsonSchemaFactory.getInstance(
          SpecVersion.VersionFlag.V202012,
          builder ->
              builder.schemaMappers(
                  mappers ->
                      mappers
                          .mapPrefix(
                              "https://raw.githubusercontent.com/apache/ossie/main/",
                              "classpath:ossie/")
                          .mapPrefix("https://github.com/apache/ossie/", "classpath:ossie/")));

  private OssieOntologyDocuments() {}

  /**
   * Parses and validates a standalone Ossie ontology.
   *
   * @param document The input document.
   * @return A validated ontology suitable for creation.
   */
  public static ParsedOntology importOntology(OssieDocument document) {
    ObjectNode root = read(document, Kind.ONTOLOGY);
    ObjectNode annotations = (ObjectNode) root.get(ANNOTATIONS);
    JsonNode stored = annotations == null ? null : annotations.remove(ONTOLOGY_PROPERTIES);
    if (stored != null && annotations.isEmpty()) {
      root.remove(ANNOTATIONS);
    }
    Map<String, String> properties =
        readProperties(root, stored, "$.annotations." + ONTOLOGY_PROPERTIES);
    return new ParsedOntology(
        root.path("name").asText(),
        comment(root),
        OntologyDefinition.of(readDefinition(root, Kind.ONTOLOGY)),
        properties);
  }

  /**
   * Exports an ontology to a standalone Ossie document.
   *
   * @param ontology The current ontology.
   * @param format The output format.
   * @return The exported document.
   */
  public static OssieDocument exportOntology(Ontology ontology, OssieFormat format) {
    return write(
        ontologyRoot(
            ontology.name(), ontology.comment(), ontology.definition(), ontology.properties()),
        Kind.ONTOLOGY,
        format);
  }

  /**
   * Validates a native ontology against the pinned Ossie document shape.
   *
   * @param name The ontology name.
   * @param comment The optional ontology description.
   * @param definition The native definition.
   * @param properties The properties to export.
   */
  public static void validateOntology(
      String name,
      @Nullable String comment,
      OntologyDefinition definition,
      Map<String, String> properties) {
    validate(ontologyRoot(name, comment, definition, properties), Kind.ONTOLOGY);
  }

  private static ObjectMapper configure(ObjectMapper mapper) {
    return mapper
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  }

  private static ObjectNode ontologyRoot(
      String name,
      @Nullable String comment,
      OntologyDefinition definition,
      Map<String, String> properties) {
    ObjectNode root = root(Kind.ONTOLOGY, name, comment, definition.fields(), properties);
    ObjectNode props = propertiesNode(properties);
    ObjectNode annotations = objectField(root, ANNOTATIONS);
    if (annotations != null && annotations.has(ONTOLOGY_PROPERTIES)) {
      throw invalid("$.annotations." + ONTOLOGY_PROPERTIES + " is reserved");
    }
    if (!props.isEmpty()) {
      if (annotations == null) {
        annotations = root.putObject(ANNOTATIONS);
      }
      annotations.set(ONTOLOGY_PROPERTIES, props);
    }
    return root;
  }

  private static ObjectNode root(
      Kind kind,
      String name,
      @Nullable String description,
      Map<String, Object> fields,
      Map<String, String> properties) {
    ObjectNode root = JSON.createObjectNode();
    root.put("name", name);
    String version = properties.getOrDefault(VERSION_PROPERTY, DEFAULT_VERSION);
    if (StringUtils.isBlank(version)) {
      throw invalid("$.version must be non-blank");
    }
    root.put("version", version);
    if (description != null) {
      root.put("description", description);
    }
    ObjectNode definition = (ObjectNode) remap(JSON.valueToTree(fields), false, "$");
    definition
        .properties()
        .forEach(
            entry -> {
              if (!kind.definitionFields.contains(entry.getKey())) {
                throw invalid("$." + entry.getKey() + " is not a definition field");
              }
              root.set(entry.getKey(), entry.getValue());
            });
    return root;
  }

  private static ObjectNode read(OssieDocument document, Kind kind) {
    JsonNode input;
    try {
      input = (document.format() == OssieFormat.JSON ? JSON : YAML).readTree(document.content());
    } catch (JsonProcessingException e) {
      throw invalid("$: " + e.getOriginalMessage());
    }
    if (input == null || !input.isObject()) {
      throw invalid("$ must be an object");
    }
    ObjectNode root = (ObjectNode) input;
    if (root.has("ontology_mappings")) {
      throw invalid("$.ontology_mappings is deprecated; import a standalone mapping");
    }
    validate(root, kind);
    if (StringUtils.isBlank(root.path("version").asText())
        || StringUtils.isBlank(root.path("name").asText())) {
      throw invalid("$.version and $.name must be non-blank");
    }
    return root;
  }

  private static OssieDocument write(ObjectNode root, Kind kind, OssieFormat format) {
    validate(root, kind);
    try {
      if (format == OssieFormat.JSON) {
        return OssieDocument.json(
            JSON.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n");
      }
      return OssieDocument.yaml(YAML.writeValueAsString(root));
    } catch (IOException e) {
      throw invalid("$: Cannot serialize Ossie document: " + e.getMessage());
    }
  }

  private static void validate(ObjectNode root, Kind kind) {
    Set<ValidationMessage> problems = kind.schema.validate(root);
    if (!problems.isEmpty()) {
      throw invalid(problems.iterator().next().getMessage());
    }
  }

  private static Map<String, Object> readDefinition(ObjectNode root, Kind kind) {
    ObjectNode definition = JSON.createObjectNode();
    for (String key : kind.definitionFields) {
      if (root.has(key)) {
        definition.set(key, root.get(key));
      }
    }
    return JSON.convertValue(remap(definition, true, "$"), FIELDS);
  }

  private static JsonNode remap(JsonNode node, boolean toNative, String path) {
    if (node.isArray()) {
      ArrayNode result = JSON.createArrayNode();
      for (int i = 0; i < node.size(); i++) {
        result.add(remap(node.get(i), toNative, path + "[" + i + "]"));
      }
      return result;
    }
    if (!node.isObject()) {
      return node;
    }
    ObjectNode result = JSON.createObjectNode();
    node.properties()
        .forEach(
            entry -> {
              String name = entry.getKey();
              String childPath = path + "." + name;
              if (!toNative && OSSIE_TO_NATIVE.containsKey(name)) {
                throw invalid(childPath + " must use its Gravitino camel-case spelling");
              }
              String mapped =
                  (toNative ? OSSIE_TO_NATIVE : OSSIE_TO_NATIVE.inverse()).getOrDefault(name, name);
              result.set(
                  mapped,
                  OPAQUE_FIELDS.contains(name)
                      ? entry.getValue()
                      : remap(entry.getValue(), toNative, childPath));
            });
    return result;
  }

  @Nullable
  private static ObjectNode objectField(ObjectNode root, String field) {
    JsonNode node = root.get(field);
    if (node != null && !node.isObject()) {
      throw invalid("$." + field + " must be an object");
    }
    return (ObjectNode) node;
  }

  private static ObjectNode propertiesNode(Map<String, String> properties) {
    ObjectNode node = JSON.createObjectNode();
    properties.forEach(
        (key, value) -> {
          if (!key.equals(VERSION_PROPERTY)) {
            node.put(key, value);
          }
        });
    return node;
  }

  private static Map<String, String> readProperties(
      ObjectNode root, @Nullable JsonNode stored, String path) {
    Map<String, String> properties = new LinkedHashMap<>();
    if (stored != null) {
      if (!stored.isObject()) {
        throw invalid(path + " must be an object of string properties");
      }
      stored
          .properties()
          .forEach(
              entry -> {
                if (!entry.getValue().isTextual()) {
                  throw invalid(path + "." + entry.getKey() + " must be a string");
                }
                properties.put(entry.getKey(), entry.getValue().asText());
              });
    }
    properties.put(VERSION_PROPERTY, root.path("version").asText());
    return ImmutableMap.copyOf(properties);
  }

  @Nullable
  private static String comment(ObjectNode root) {
    return root.has("description") ? root.path("description").asText() : null;
  }

  private static IllegalArgumentException invalid(String detail) {
    return new IllegalOntologyException("%s", detail);
  }

  private static final class Kind {
    private static final Kind ONTOLOGY =
        new Kind(
            "ontology.json",
            ImmutableSet.of("ai_context", "prefixes", "requires", ANNOTATIONS, "ontology"));

    private final JsonSchema schema;
    private final Set<String> definitionFields;

    Kind(String file, Set<String> definitionFields) {
      this.definitionFields = definitionFields;
      this.schema =
          SCHEMAS.getSchema(
              SchemaLocation.of("classpath:ossie/ontology/" + file),
              SchemaValidatorsConfig.builder()
                  .pathType(PathType.JSON_PATH)
                  .formatAssertionsEnabled(true)
                  .build());
    }
  }
  /** A parsed Ossie ontology before it has been persisted. */
  public static final class ParsedOntology {
    private final String name;
    @Nullable private final String comment;
    private final OntologyDefinition definition;
    private final Map<String, String> properties;

    private ParsedOntology(
        String name,
        @Nullable String comment,
        OntologyDefinition definition,
        Map<String, String> properties) {
      this.name = name;
      this.comment = comment;
      this.definition = definition;
      this.properties = properties;
    }

    /**
     * @return The name from the Ossie document.
     */
    public String name() {
      return name;
    }

    /**
     * @return The optional description.
     */
    public @Nullable String comment() {
      return comment;
    }

    /**
     * @return The native ontology definition.
     */
    public OntologyDefinition definition() {
      return definition;
    }

    /**
     * @return The properties including the source version.
     */
    public Map<String, String> properties() {
      return properties;
    }
  }
}
