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

import static org.apache.gravitino.ontology.Ontology.PROPERTY_OSSIE_VERSION;
import static org.apache.gravitino.semantic.OssieVersion.DEFAULT_VERSION;

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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;

/**
 * Converts between standalone Apache Ossie ontology documents and Gravitino {@link Ontology}
 * values.
 *
 * <p>An ontology document is validated against the closed root, component and relationship property
 * sets of {@code ontology/ontology.json}, not against the Semantic Model core document: the two
 * documents share a {@code version}, {@code name} and {@code description} shape and nothing else.
 * The validator is written rather than delegated to a JSON Schema library, as the Semantic Model
 * converter is, so the same rules produce the same messages in every deployment.
 *
 * <p>Two rules are checked beyond the schema's own keywords: a relationship must carry at least one
 * non-blank {@code verbalizes} phrasing, and every concept a component names in {@code extends} or
 * in a relationship role must be declared in the same document. The document has no place for
 * external references, so a dangling name is rejected rather than accepted and resolved later.
 *
 * <p>The converter also owns the internal metadata-store representation of a definition, through
 * {@link #toDefinitionJson(OntologyDefinition)} and {@link #fromDefinitionJson(String)}. That
 * representation holds the definition sections only and stays separate from the interchange
 * document, so the metadata store never has to know the Apache Ossie component and relationship
 * shapes.
 */
public final class OssieOntologyConverter {

  private static final Set<String> ROOT_PROPERTIES =
      Set.of("version", "name", "description", "ai_context", "requires", "ontology", "prefixes");
  private static final Set<String> DEFINITION_PROPERTIES =
      Set.of("ai_context", "requires", "ontology", "prefixes");
  private static final Set<String> COMPONENT_PROPERTIES =
      Set.of(
          "concept",
          "type",
          "description",
          "extends",
          "derived_by",
          "identify_by",
          "requires",
          "relationships",
          "iri");
  private static final Set<String> RELATIONSHIP_PROPERTIES =
      Set.of(
          "name",
          "description",
          "roles",
          "multiplicity",
          "derived_by",
          "requires",
          "verbalizes",
          "iri");
  private static final Set<String> ROLE_PROPERTIES = Set.of("concept", "name");
  private static final Set<String> AI_CONTEXT_PROPERTIES =
      Set.of("instructions", "synonyms", "examples");

  private static final Map<String, ConceptType> CONCEPT_TYPES =
      Map.of("EntityType", ConceptType.ENTITY_TYPE, "ValueType", ConceptType.VALUE_TYPE);
  private static final Map<String, Multiplicity> MULTIPLICITIES =
      Map.of("ManyToOne", Multiplicity.MANY_TO_ONE, "OneToOne", Multiplicity.ONE_TO_ONE);

  private static final ObjectMapper JSON_MAPPER = createJsonMapper();
  private static final ObjectMapper YAML_MAPPER = createYamlMapper();

  private OssieOntologyConverter() {}

  /**
   * Converts one standalone Apache Ossie YAML or JSON ontology document into native ontology
   * values.
   *
   * @param document The standalone Ossie ontology document.
   * @return The ontology name, comment, definition and properties.
   * @throws IllegalOntologyException If the document cannot be parsed or is not a valid ontology.
   */
  public static ParsedOntology importDocument(OssieDocument document) {
    Objects.requireNonNull(document, "document must not be null");
    return toParsedOntology(parseDocument(document), false);
  }

  /**
   * Exports one Gravitino ontology as a standalone Apache Ossie ontology document.
   *
   * @param ontology The ontology to export.
   * @param format The requested serialization format.
   * @return The serialized Ossie ontology document.
   * @throws IllegalOntologyException If the ontology cannot be represented as a valid document.
   */
  public static OssieDocument exportDocument(Ontology ontology, OssieFormat format) {
    Objects.requireNonNull(ontology, "ontology must not be null");
    Objects.requireNonNull(format, "format must not be null");

    ObjectNode root = JSON_MAPPER.createObjectNode();
    root.put("version", ossieVersion(ontology.properties()));
    root.put("name", ontology.name());
    if (ontology.comment() != null) {
      root.put("description", ontology.comment());
    }
    writeDefinition(root, ontology.definition(), false);

    // Validate the generated document through the same conversion path used for imports.
    toParsedOntology(root.deepCopy(), false);

    try {
      if (format == OssieFormat.JSON) {
        return OssieDocument.json(
            JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n");
      }
      return OssieDocument.yaml(YAML_MAPPER.writeValueAsString(root));
    } catch (JsonProcessingException e) {
      throw new IllegalOntologyException(
          e, "Cannot serialize Apache Ossie ontology document: %s", e.getOriginalMessage());
    }
  }

  /**
   * Serializes one ontology definition into the internal metadata-store representation.
   *
   * <p>This is a storage representation, not an Apache Ossie interchange document: it carries only
   * the definition sections ({@code ai_context}, {@code requires}, {@code ontology} and {@code
   * prefixes}) and has no {@code version}, {@code name} or {@code description}, because the
   * metadata store keeps those on the ontology identity row and its version snapshot instead. Use
   * {@link #importDocument(OssieDocument)} and {@link #exportDocument(Ontology, OssieFormat)} for
   * interchange.
   *
   * <p>The output is deterministic: the object keys are written in a fixed order and the concepts
   * stay in the order the definition declares them, so the same definition always serializes to the
   * same text.
   *
   * @param definition The definition to serialize.
   * @return The serialized definition, as a JSON object.
   */
  public static String toDefinitionJson(OntologyDefinition definition) {
    Objects.requireNonNull(definition, "definition must not be null");

    ObjectNode root = JSON_MAPPER.createObjectNode();
    writeDefinition(root, definition, true);
    try {
      return JSON_MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new IllegalOntologyException(
          e, "Cannot serialize ontology definition: %s", e.getOriginalMessage());
    }
  }

  /**
   * Reads one ontology definition from the internal metadata-store representation written by {@link
   * #toDefinitionJson(OntologyDefinition)}.
   *
   * <p>This is the exact inverse of {@link #toDefinitionJson(OntologyDefinition)}, so {@code
   * fromDefinitionJson(toDefinitionJson(definition))} equals {@code definition}. It is a storage
   * representation, not an Apache Ossie interchange document: it has no {@code version}, {@code
   * name} or {@code description}, and those properties are rejected like any other property the
   * definition sections do not declare.
   *
   * @param json The serialized definition.
   * @return The definition.
   * @throws IllegalOntologyException If the text is not a JSON object, declares a property outside
   *     the definition sections, or describes an invalid ontology.
   */
  public static OntologyDefinition fromDefinitionJson(String json) {
    Objects.requireNonNull(json, "json must not be null");

    JsonNode parsed;
    try {
      parsed =
          JSON_MAPPER.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(json);
    } catch (IOException e) {
      throw new IllegalOntologyException(
          e, "Cannot parse stored ontology definition: %s", originalMessage(e));
    }
    if (!(parsed instanceof ObjectNode)) {
      throw invalid("$", "stored ontology definition root must be an object");
    }

    ObjectNode root = (ObjectNode) parsed;
    validateObject(root, "$", DEFINITION_PROPERTIES);
    try {
      return readDefinition(root, true);
    } catch (IllegalOntologyException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new IllegalOntologyException(
          e,
          "Cannot convert stored JSON to a Gravitino Ontology definition: %s",
          originalMessage(e));
    }
  }

  /**
   * Assigns an identifier to every concept and relationship that does not carry one, keeping the
   * identifiers of the stored version where the name is unchanged.
   *
   * <p>The metadata store never keeps a definition without identifiers: a client names concepts and
   * relationships, and this is where Gravitino decides which incoming name is which stored object.
   * A value that already carries an identifier keeps it whatever its name, which is how a rename is
   * expressed; a value without one inherits the identifier of the same name in {@code previous},
   * and a value in neither gets a new one. A relationship is resolved inside its concept, because
   * two concepts may name a relationship the same way.
   *
   * @param definition The definition to assign identifiers to.
   * @param previous The stored definition this one replaces, or {@code null} when the ontology is
   *     being created.
   * @return A definition in which every concept and relationship carries an identifier.
   */
  public static OntologyDefinition assignIds(
      OntologyDefinition definition, @Nullable OntologyDefinition previous) {
    Objects.requireNonNull(definition, "definition must not be null");

    Map<String, OntologyConcept> storedConcepts = new LinkedHashMap<>();
    if (previous != null) {
      for (OntologyConcept concept : previous.concepts()) {
        storedConcepts.put(concept.concept(), concept);
      }
    }

    OntologyConcept[] concepts = definition.concepts();
    OntologyConcept[] assigned = new OntologyConcept[concepts.length];
    for (int index = 0; index < concepts.length; index++) {
      OntologyConcept concept = concepts[index];
      OntologyConcept stored = storedConcepts.get(concept.concept());
      String id = concept.id();
      if (id == null && stored != null) {
        id = stored.id();
      }
      assigned[index] =
          copy(concept, id == null ? newId() : id, assignRelationshipIds(concept, stored));
    }

    return OntologyDefinition.builder()
        .withAiContext(definition.aiContext())
        .withRequires(definition.requires())
        .withConcepts(assigned)
        .withPrefixes(definition.prefixes())
        .build();
  }

  @Nullable
  private static OntologyRelationship[] assignRelationshipIds(
      OntologyConcept concept, @Nullable OntologyConcept stored) {
    OntologyRelationship[] relationships = concept.relationships();
    if (relationships == null) {
      return null;
    }

    Map<String, OntologyRelationship> storedRelationships = new LinkedHashMap<>();
    if (stored != null && stored.relationships() != null) {
      for (OntologyRelationship relationship : stored.relationships()) {
        storedRelationships.put(relationship.name(), relationship);
      }
    }

    OntologyRelationship[] assigned = new OntologyRelationship[relationships.length];
    for (int index = 0; index < relationships.length; index++) {
      OntologyRelationship relationship = relationships[index];
      String id = relationship.id();
      if (id == null) {
        OntologyRelationship old = storedRelationships.get(relationship.name());
        id = old == null ? null : old.id();
      }
      assigned[index] = copy(relationship, id == null ? newId() : id);
    }
    return assigned;
  }

  private static OntologyConcept copy(
      OntologyConcept concept, String id, @Nullable OntologyRelationship[] relationships) {
    return OntologyConcept.builder()
        .withId(id)
        .withConcept(concept.concept())
        .withType(concept.type())
        .withDescription(concept.description())
        .withExtendsConcepts(concept.extendsConcepts())
        .withDerivedBy(concept.derivedBy())
        .withIdentifyBy(concept.identifyBy())
        .withRequires(concept.requires())
        .withRelationships(relationships)
        .withIri(concept.iri())
        .build();
  }

  private static OntologyRelationship copy(OntologyRelationship relationship, String id) {
    return OntologyRelationship.builder()
        .withId(id)
        .withName(relationship.name())
        .withDescription(relationship.description())
        .withVerbalizes(relationship.verbalizes())
        .withRoles(relationship.roles())
        .withMultiplicity(relationship.multiplicity())
        .withDerivedBy(relationship.derivedBy())
        .withRequires(relationship.requires())
        .withIri(relationship.iri())
        .build();
  }

  /**
   * Immutable native ontology values parsed from an Ossie ontology document. Parsing does not check
   * for an existing ontology with the same name and does not create anything.
   */
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
      this.properties = Map.copyOf(properties);
    }

    /**
     * Returns the ontology name supplied by the document.
     *
     * @return The ontology name.
     */
    public String name() {
      return name;
    }

    /**
     * Returns the document description, which becomes the ontology comment.
     *
     * @return The comment, or {@code null} when absent.
     */
    @Nullable
    public String comment() {
      return comment;
    }

    /**
     * Returns the parsed concept graph.
     *
     * @return The immutable ontology definition.
     */
    public OntologyDefinition definition() {
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
      JsonNode parsed =
          mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(content);
      if (!(parsed instanceof ObjectNode)) {
        throw invalid("$", "document root must be an object");
      }
      return (ObjectNode) parsed;
    } catch (IOException e) {
      throw new IllegalOntologyException(
          e, "Cannot parse Apache Ossie ontology %s: %s", document.format(), originalMessage(e));
    }
  }

  private static ParsedOntology toParsedOntology(ObjectNode root, boolean fromStore) {
    if (root.has("ontology_mappings")) {
      throw invalid(
          "$.ontology_mappings",
          "embedded ontology mappings are deprecated by Apache Ossie; write a standalone mapping"
              + " document validated against ontology/mapping.json instead");
    }
    validateObject(root, "$", ROOT_PROPERTIES);

    String name = readText(root, "name", "$");
    String comment = readText(root, "description", "$");
    String ossieVersion = validateVersion(root.get("version"));

    try {
      if (StringUtils.isBlank(name)) {
        throw new IllegalArgumentException("\"name\" field is required and cannot be empty");
      }
      Map<String, String> properties = new LinkedHashMap<>();
      properties.put(PROPERTY_OSSIE_VERSION, ossieVersion);
      return new ParsedOntology(name, comment, readDefinition(root, fromStore), properties);
    } catch (IllegalOntologyException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new IllegalOntologyException(
          e,
          "Cannot convert Apache Ossie document to a Gravitino Ontology: %s",
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
          "$.version", "Ontology property '" + PROPERTY_OSSIE_VERSION + "' must not be blank");
    }
    return version;
  }

  private static OntologyDefinition readDefinition(ObjectNode root, boolean fromStore) {
    Map<String, String> prefixes = readPrefixes(root.get("prefixes"), "$.prefixes");
    OntologyConcept[] concepts =
        readObjectArray(
            root.get("ontology"),
            "$.ontology",
            (component, path) -> readComponent(component, path, prefixes, fromStore),
            OntologyConcept[]::new);

    return OntologyDefinition.builder()
        .withAiContext(readAiContext(root.get("ai_context"), "$.ai_context"))
        .withRequires(readStringArray(root, "requires", "$"))
        .withConcepts(concepts)
        .withPrefixes(prefixes)
        .build();
  }

  private static OntologyConcept readComponent(
      ObjectNode component, String path, Map<String, String> prefixes, boolean fromStore) {
    validateObject(component, path, COMPONENT_PROPERTIES, fromStore);
    String concept = readText(component, "concept", path);
    return OntologyConcept.builder()
        .withId(fromStore ? readId(component, path) : null)
        .withConcept(concept)
        .withType(readConceptType(component.get("type"), path + ".type"))
        .withDescription(readText(component, "description", path))
        .withExtendsConcepts(readStringArray(component, "extends", path))
        .withDerivedBy(readStringArray(component, "derived_by", path))
        .withIdentifyBy(readStringArray(component, "identify_by", path))
        .withRequires(readStringArray(component, "requires", path))
        .withRelationships(
            readObjectArray(
                component.get("relationships"),
                path + ".relationships",
                (relationship, relationshipPath) ->
                    readRelationship(relationship, relationshipPath, prefixes, fromStore),
                OntologyRelationship[]::new))
        .withIri(readIri(component, path, prefixes))
        .build();
  }

  private static OntologyRelationship readRelationship(
      ObjectNode relationship, String path, Map<String, String> prefixes, boolean fromStore) {
    validateObject(relationship, path, RELATIONSHIP_PROPERTIES, fromStore);
    return OntologyRelationship.builder()
        .withId(fromStore ? readId(relationship, path) : null)
        .withName(readText(relationship, "name", path))
        .withDescription(readText(relationship, "description", path))
        .withVerbalizes(readStringArray(relationship, "verbalizes", path))
        .withRoles(
            readObjectArray(
                relationship.get("roles"),
                path + ".roles",
                OssieOntologyConverter::readRole,
                OntologyRole[]::new))
        .withMultiplicity(
            readMultiplicity(relationship.get("multiplicity"), path + ".multiplicity"))
        .withDerivedBy(readStringArray(relationship, "derived_by", path))
        .withRequires(readStringArray(relationship, "requires", path))
        .withIri(readIri(relationship, path, prefixes))
        .build();
  }

  private static OntologyRole readRole(ObjectNode role, String path) {
    validateObject(role, path, ROLE_PROPERTIES);
    return OntologyRole.builder()
        .withConcept(readText(role, "concept", path))
        .withName(readText(role, "name", path))
        .build();
  }

  private static ConceptType readConceptType(@Nullable JsonNode node, String path) {
    if (node == null || !node.isTextual()) {
      throw invalid(path, "must be one of " + CONCEPT_TYPES.keySet());
    }
    ConceptType type = CONCEPT_TYPES.get(node.textValue());
    if (type == null) {
      throw invalid(path, "must be one of " + CONCEPT_TYPES.keySet());
    }
    return type;
  }

  @Nullable
  private static Multiplicity readMultiplicity(@Nullable JsonNode node, String path) {
    if (node == null) {
      return null;
    }
    if (!node.isTextual()) {
      throw invalid(path, "must be one of " + MULTIPLICITIES.keySet());
    }
    Multiplicity multiplicity = MULTIPLICITIES.get(node.textValue());
    if (multiplicity == null) {
      throw invalid(path, "must be one of " + MULTIPLICITIES.keySet());
    }
    return multiplicity;
  }

  @Nullable
  private static String readIri(ObjectNode object, String path, Map<String, String> prefixes) {
    String iri = readText(object, "iri", path);
    if (iri == null) {
      return null;
    }
    if (StringUtils.isBlank(iri)) {
      throw invalid(path + ".iri", "must not be blank");
    }
    if (!iri.contains(":")) {
      throw invalid(path + ".iri", "must be a full IRI or a QName resolved against $.prefixes");
    }
    // An absolute IRI carries its own scheme; a QName is prefix:local and needs a declared prefix.
    boolean absolute = iri.contains("://") || iri.startsWith("urn:");
    String prefix = iri.substring(0, iri.indexOf(':'));
    if (!absolute && !prefixes.containsKey(prefix)) {
      throw invalid(path + ".iri", "QName prefix '" + prefix + "' is not declared in $.prefixes");
    }
    return iri;
  }

  private static Map<String, String> readPrefixes(@Nullable JsonNode node, String path) {
    Map<String, String> prefixes = new LinkedHashMap<>();
    if (node == null) {
      return prefixes;
    }
    if (!(node instanceof ObjectNode)) {
      throw invalid(path, "must be an object of prefix to IRI");
    }
    node.fields()
        .forEachRemaining(
            entry -> {
              if (StringUtils.isBlank(entry.getKey())) {
                throw invalid(path, "must not declare a blank prefix");
              }
              if (!entry.getValue().isTextual()) {
                throw invalid(path + "." + entry.getKey(), "must be a string");
              }
              prefixes.put(entry.getKey(), entry.getValue().textValue());
            });
    return prefixes;
  }

  private static void writeDefinition(
      ObjectNode root, OntologyDefinition definition, boolean includeIds) {
    putOptional(root, "ai_context", writeAiContext(definition.aiContext()));
    if (definition.requires().length > 0) {
      putOptional(root, "requires", definition.requires());
    }

    ArrayNode ontology = root.putArray("ontology");
    for (OntologyConcept concept : definition.concepts()) {
      ontology.add(writeComponent(concept, includeIds));
    }

    if (!definition.prefixes().isEmpty()) {
      ObjectNode prefixes = root.putObject("prefixes");
      definition.prefixes().forEach(prefixes::put);
    }
  }

  private static ObjectNode writeComponent(OntologyConcept concept, boolean includeIds) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    putOptional(node, "id", includeIds ? concept.id() : null);
    node.put("concept", concept.concept());
    node.put("type", conceptTypeName(concept.type()));
    putOptional(node, "description", concept.description());
    putOptional(node, "extends", concept.extendsConcepts());
    putOptional(node, "derived_by", concept.derivedBy());
    putOptional(node, "identify_by", concept.identifyBy());
    putOptional(node, "requires", concept.requires());
    OntologyRelationship[] relationships = concept.relationships();
    if (relationships != null) {
      ArrayNode relationshipArray = node.putArray("relationships");
      for (OntologyRelationship relationship : relationships) {
        relationshipArray.add(writeRelationship(relationship, includeIds));
      }
    }
    putOptional(node, "iri", concept.iri());
    return node;
  }

  private static ObjectNode writeRelationship(
      OntologyRelationship relationship, boolean includeIds) {
    ObjectNode node = JSON_MAPPER.createObjectNode();
    putOptional(node, "id", includeIds ? relationship.id() : null);
    node.put("name", relationship.name());
    putOptional(node, "description", relationship.description());
    OntologyRole[] roles = relationship.roles();
    if (roles != null) {
      ArrayNode roleArray = node.putArray("roles");
      for (OntologyRole role : roles) {
        ObjectNode roleNode = roleArray.addObject();
        roleNode.put("concept", role.concept());
        putOptional(roleNode, "name", role.name());
      }
    }
    if (relationship.multiplicity() != null) {
      node.put("multiplicity", multiplicityName(relationship.multiplicity()));
    }
    putOptional(node, "derived_by", relationship.derivedBy());
    putOptional(node, "requires", relationship.requires());
    putOptional(node, "verbalizes", relationship.verbalizes());
    putOptional(node, "iri", relationship.iri());
    return node;
  }

  private static String conceptTypeName(ConceptType type) {
    switch (type) {
      case ENTITY_TYPE:
        return "EntityType";
      case VALUE_TYPE:
        return "ValueType";
      default:
        throw invalid("$.ontology.type", "unknown concept type " + type);
    }
  }

  private static String multiplicityName(Multiplicity multiplicity) {
    switch (multiplicity) {
      case MANY_TO_ONE:
        return "ManyToOne";
      case ONE_TO_ONE:
        return "OneToOne";
      default:
        throw invalid("$.ontology.multiplicity", "unknown multiplicity " + multiplicity);
    }
  }

  private static AIContext readAiContext(@Nullable JsonNode node, String path) {
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

  @Nullable
  private static JsonNode writeAiContext(@Nullable AIContext context) {
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

  private static void validateObject(ObjectNode object, String path, Set<String> allowed) {
    validateObject(object, path, allowed, false);
  }

  private static void validateObject(
      ObjectNode object, String path, Set<String> allowed, boolean allowId) {
    object
        .fields()
        .forEachRemaining(
            entry -> {
              if (allowId && "id".equals(entry.getKey())) {
                return;
              }
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

  /**
   * Reads the identifier of a stored component or relationship, minting one when the stored form
   * predates identifiers. The value is never written to an Apache Ossie document.
   */
  private static String readId(ObjectNode object, String path) {
    String id = readText(object, "id", path);
    if (id != null && StringUtils.isBlank(id)) {
      throw invalid(path + ".id", "must not be blank");
    }
    return id == null ? newId() : id;
  }

  private static String newId() {
    return UUID.randomUUID().toString();
  }

  @Nullable
  private static String readText(ObjectNode object, String name, String path) {
    validateOptionalText(object, name, path);
    JsonNode value = object.get(name);
    return value == null ? null : value.textValue();
  }

  /**
   * Reads an optional string array. The builders of {@link OntologyConcept} and {@link
   * OntologyRelationship} take varargs, so a missing array is passed as {@code null} and a present
   * one as its values.
   */
  @Nullable
  private static String[] readStringArray(ObjectNode object, String name, String path) {
    validateOptionalStringArray(object, name, path);
    JsonNode value = object.get(name);
    if (value == null) {
      return null;
    }
    String[] values = new String[value.size()];
    for (int index = 0; index < values.length; index++) {
      values[index] = value.get(index).textValue();
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

  private static IllegalOntologyException invalid(String path, String detail) {
    return new IllegalOntologyException("%s: %s", path, detail);
  }

  @FunctionalInterface
  private interface ObjectReader<T> {
    T read(ObjectNode object, String path);
  }
}
