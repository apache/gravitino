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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.exceptions.IllegalOntologyMappingException;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;

/**
 * Converts standalone Ossie ontology and mapping documents using the bundled schema pin.
 *
 * <p>The schemas under {@code ossie/} are copied from <a
 * href="https://github.com/apache/ossie/tree/3719487ff2744b207d0cc9bed66f5406f410dc3d">Apache Ossie
 * at 3719487</a>, and every schema reference resolves against these bundled resources. Gravitino
 * accepts any nonempty document version, so the root {@code version} constraints in {@code
 * ontology/ontology.json} and {@code ontology/mapping.json} use {@code minLength: 1}. Re-apply that
 * change when the schemas are updated from upstream; {@code
 * TestOssieOntologyDocuments#testAnyNonBlankVersionIsAccepted} fails otherwise.
 */
public final class OssieOntologyDocuments {

  /** The Ossie version for new native ontologies and mappings. */
  public static final String DEFAULT_VERSION = "0.3.0.dev0";

  /** The property that carries the version written to an Ossie document. */
  public static final String VERSION_PROPERTY = "ossie-version";

  private static final String ANNOTATIONS = "annotations";
  private static final String ONTOLOGY_PROPERTIES = "gravitino_properties";
  private static final String MAPPING_PROPERTIES = "GRAVITINO_PROPERTIES";
  private static final String EXTENSIONS = "nonportable_extensions";
  private static final TypeReference<Map<String, Object>> FIELDS =
      new TypeReference<Map<String, Object>>() {};
  private static final Set<String> OPAQUE_FIELDS =
      ImmutableSet.of(ANNOTATIONS, "prefixes", "data", "ai_context", "aiContext");
  private static final ImmutableBiMap<String, String> OSSIE_TO_NATIVE =
      ImmutableBiMap.<String, String>builder()
          .put("ai_context", "aiContext")
          .put("identify_by", "identifyBy")
          .put("derived_by", "derivedBy")
          .put("concept_mappings", "conceptMappings")
          .put("object_mappings", "objectMappings")
          .put("link_mappings", "linkMappings")
          .put("object_mapping", "objectMapping")
          .put("referent_mappings", "referentMappings")
          .put("nonportable_extensions", "nonportableExtensions")
          .put("vendor_name", "vendorName")
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
        readProperties(root, stored, Kind.ONTOLOGY, "$.annotations." + ONTOLOGY_PROPERTIES);
    return new ParsedOntology(
        root.path("name").asText(),
        comment(root),
        OntologyDefinition.of(readDefinition(root, Kind.ONTOLOGY)),
        properties);
  }

  /**
   * Parses a standalone Ossie mapping and checks that the declared model is its actual parent.
   *
   * @param document The input document.
   * @param semanticModel The name of the mapping's parent.
   * @return A structurally validated mapping suitable for creation after reference checks.
   */
  public static ParsedMapping importMapping(OssieDocument document, String semanticModel) {
    ObjectNode root = read(document, Kind.MAPPING);
    if (!semanticModel.equals(root.path("semantic_model_ref").path("name").asText())) {
      throw invalid(Kind.MAPPING, "$.semantic_model_ref.name must equal the parent semantic model");
    }
    ArrayNode extensions = (ArrayNode) root.get(EXTENSIONS);
    JsonNode stored = removeMappingProperties(extensions);
    if (stored != null && extensions.isEmpty()) {
      root.remove(EXTENSIONS);
    }
    Map<String, String> properties = readProperties(root, stored, Kind.MAPPING, "$." + EXTENSIONS);
    return new ParsedMapping(
        root.path("name").asText(),
        comment(root),
        root.path("ontology_ref").path("name").asText(),
        OntologyMappingDefinition.of(readDefinition(root, Kind.MAPPING)),
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
   * Exports a mapping, using the current names of both referenced objects.
   *
   * @param mapping The current mapping.
   * @param semanticModel The name of its parent.
   * @param format The output format.
   * @return The exported document.
   */
  public static OssieDocument exportMapping(
      OntologyMapping mapping, String semanticModel, OssieFormat format) {
    return write(
        mappingRoot(
            mapping.name(),
            mapping.comment(),
            mapping.ontology(),
            semanticModel,
            mapping.definition(),
            mapping.properties()),
        Kind.MAPPING,
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

  /**
   * Validates a native mapping against the pinned Ossie document shape.
   *
   * @param name The mapping name.
   * @param comment The optional mapping description.
   * @param ontology The ontology name.
   * @param semanticModel The parent model name.
   * @param definition The native definition.
   * @param properties The properties to export.
   */
  public static void validateMapping(
      String name,
      @Nullable String comment,
      String ontology,
      String semanticModel,
      OntologyMappingDefinition definition,
      Map<String, String> properties) {
    validate(
        mappingRoot(name, comment, ontology, semanticModel, definition, properties), Kind.MAPPING);
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
    ObjectNode annotations = objectField(root, ANNOTATIONS, Kind.ONTOLOGY);
    if (annotations != null && annotations.has(ONTOLOGY_PROPERTIES)) {
      throw invalid(Kind.ONTOLOGY, "$.annotations." + ONTOLOGY_PROPERTIES + " is reserved");
    }
    if (!props.isEmpty()) {
      if (annotations == null) {
        annotations = root.putObject(ANNOTATIONS);
      }
      annotations.set(ONTOLOGY_PROPERTIES, props);
    }
    return root;
  }

  private static ObjectNode mappingRoot(
      String name,
      @Nullable String comment,
      String ontology,
      String semanticModel,
      OntologyMappingDefinition definition,
      Map<String, String> properties) {
    ObjectNode root = root(Kind.MAPPING, name, comment, definition.fields(), properties);
    root.putObject("ontology_ref").put("name", ontology);
    root.putObject("semantic_model_ref").put("name", semanticModel);
    JsonNode extensions = root.get(EXTENSIONS);
    if (extensions != null && !extensions.isArray()) {
      throw invalid(Kind.MAPPING, "$." + EXTENSIONS + " must be an array");
    }
    if (extensions != null) {
      for (JsonNode extension : extensions) {
        if (MAPPING_PROPERTIES.equals(extension.path("vendor_name").asText())) {
          throw invalid(Kind.MAPPING, "$." + EXTENSIONS + " contains a reserved vendor");
        }
      }
    }
    ObjectNode props = propertiesNode(properties);
    if (!props.isEmpty()) {
      ArrayNode array = extensions == null ? root.putArray(EXTENSIONS) : (ArrayNode) extensions;
      array.addObject().put("vendor_name", MAPPING_PROPERTIES).set("data", props);
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
      throw invalid(kind, "$.version must be non-blank");
    }
    root.put("version", version);
    if (description != null) {
      root.put("description", description);
    }
    ObjectNode definition = (ObjectNode) remap(JSON.valueToTree(fields), kind, false, "$");
    definition
        .fields()
        .forEachRemaining(
            entry -> {
              if (!kind.definitionFields.contains(entry.getKey())) {
                throw invalid(kind, "$." + entry.getKey() + " is not a definition field");
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
      throw invalid(kind, "$: " + e.getOriginalMessage());
    }
    if (input == null || !input.isObject()) {
      throw invalid(kind, "$ must be an object");
    }
    ObjectNode root = (ObjectNode) input;
    if (kind == Kind.ONTOLOGY && root.has("ontology_mappings")) {
      throw invalid(kind, "$.ontology_mappings is deprecated; import a standalone mapping");
    }
    validate(root, kind);
    if (StringUtils.isBlank(root.path("version").asText())
        || StringUtils.isBlank(root.path("name").asText())) {
      throw invalid(kind, "$.version and $.name must be non-blank");
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
      throw invalid(kind, "$: Cannot serialize Ossie document: " + e.getMessage());
    }
  }

  private static void validate(ObjectNode root, Kind kind) {
    Set<ValidationMessage> problems = kind.schema.validate(root);
    if (!problems.isEmpty()) {
      throw invalid(kind, problems.iterator().next().getMessage());
    }
  }

  private static Map<String, Object> readDefinition(ObjectNode root, Kind kind) {
    ObjectNode definition = JSON.createObjectNode();
    for (String key : kind.definitionFields) {
      if (root.has(key)) {
        definition.set(key, root.get(key));
      }
    }
    return JSON.convertValue(remap(definition, kind, true, "$"), FIELDS);
  }

  private static JsonNode remap(JsonNode node, Kind kind, boolean toNative, String path) {
    if (node.isArray()) {
      ArrayNode result = JSON.createArrayNode();
      for (int i = 0; i < node.size(); i++) {
        result.add(remap(node.get(i), kind, toNative, path + "[" + i + "]"));
      }
      return result;
    }
    if (!node.isObject()) {
      return node;
    }
    ObjectNode result = JSON.createObjectNode();
    node.fields()
        .forEachRemaining(
            entry -> {
              String name = entry.getKey();
              String childPath = path + "." + name;
              if (!toNative && OSSIE_TO_NATIVE.containsKey(name)) {
                throw invalid(kind, childPath + " must use its Gravitino camel-case spelling");
              }
              String mapped =
                  (toNative ? OSSIE_TO_NATIVE : OSSIE_TO_NATIVE.inverse()).getOrDefault(name, name);
              result.set(
                  mapped,
                  OPAQUE_FIELDS.contains(name)
                      ? entry.getValue()
                      : remap(entry.getValue(), kind, toNative, childPath));
            });
    return result;
  }

  @Nullable
  private static ObjectNode objectField(ObjectNode root, String field, Kind kind) {
    JsonNode node = root.get(field);
    if (node != null && !node.isObject()) {
      throw invalid(kind, "$." + field + " must be an object");
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

  @Nullable
  private static JsonNode removeMappingProperties(@Nullable ArrayNode extensions) {
    JsonNode stored = null;
    if (extensions != null) {
      Iterator<JsonNode> iter = extensions.elements();
      while (iter.hasNext()) {
        JsonNode extension = iter.next();
        if (MAPPING_PROPERTIES.equals(extension.path("vendor_name").asText())) {
          if (stored != null) {
            throw invalid(Kind.MAPPING, "$." + EXTENSIONS + " contains duplicate reserved entries");
          }
          stored = extension.get("data");
          iter.remove();
        }
      }
    }
    return stored;
  }

  private static Map<String, String> readProperties(
      ObjectNode root, @Nullable JsonNode stored, Kind kind, String path) {
    Map<String, String> properties = new LinkedHashMap<>();
    if (stored != null) {
      if (!stored.isObject()) {
        throw invalid(kind, path + " must be an object of string properties");
      }
      stored
          .fields()
          .forEachRemaining(
              entry -> {
                if (!entry.getValue().isTextual()) {
                  throw invalid(kind, path + "." + entry.getKey() + " must be a string");
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

  private static IllegalArgumentException invalid(Kind kind, String detail) {
    return kind == Kind.ONTOLOGY
        ? new IllegalOntologyException("%s", detail)
        : new IllegalOntologyMappingException("%s", detail);
  }

  private static final class Kind {
    private static final Kind ONTOLOGY =
        new Kind(
            "ontology.json",
            ImmutableSet.of("ai_context", "prefixes", "requires", ANNOTATIONS, "ontology"));
    private static final Kind MAPPING =
        new Kind("mapping.json", ImmutableSet.of("concept_mappings", EXTENSIONS));

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

  /** A parsed Ossie mapping before it has been persisted. */
  public static final class ParsedMapping {
    private final String name;
    @Nullable private final String comment;
    private final String ontology;
    private final OntologyMappingDefinition definition;
    private final Map<String, String> properties;

    private ParsedMapping(
        String name,
        @Nullable String comment,
        String ontology,
        OntologyMappingDefinition definition,
        Map<String, String> properties) {
      this.name = name;
      this.comment = comment;
      this.ontology = ontology;
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
     * @return The name of the referenced ontology.
     */
    public String ontology() {
      return ontology;
    }

    /**
     * @return The native mapping definition.
     */
    public OntologyMappingDefinition definition() {
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
