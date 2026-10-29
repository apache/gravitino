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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.semantic.AIContext;

/**
 * An immutable ontology definition: the concept graph of an {@link Ontology}, without its name,
 * comment, status or Gravitino properties.
 *
 * <p>A definition is the whole of an Apache Ossie ontology document except the root {@code name},
 * {@code description} and {@code version}. Importing a document fills it in, and exporting an
 * ontology writes it out, so the two directions round-trip.
 *
 * <p>Every concept reference resolves inside this definition. The Apache Ossie ontology document
 * has no place for external references, so an {@code extends} or a role that names a concept the
 * definition does not declare is rejected rather than left for a reader to resolve later.
 */
@Evolving
public final class OntologyDefinition {

  @Nullable private final AIContext aiContext;
  private final String[] requires;
  private final OntologyConcept[] concepts;
  private final Map<String, String> prefixes;

  private OntologyDefinition(Builder builder) {
    this.aiContext = builder.aiContext;
    this.requires = Arrays.copyOf(builder.requires, builder.requires.length);
    this.concepts = Arrays.copyOf(builder.concepts, builder.concepts.length);
    this.prefixes = Collections.unmodifiableMap(new LinkedHashMap<>(builder.prefixes));
  }

  /**
   * Creates a builder for an immutable {@link OntologyDefinition}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the AI context of the ontology.
   *
   * @return The AI context, or {@code null} if it is not set.
   */
  @Nullable
  public AIContext aiContext() {
    return aiContext;
  }

  /**
   * Returns a copy of the ANSI SQL expressions that constrain the population of the whole ontology.
   *
   * @return The non-null array of expressions, empty if none are set.
   */
  public String[] requires() {
    return Arrays.copyOf(requires, requires.length);
  }

  /**
   * Returns a copy of the concepts of the ontology.
   *
   * @return The non-empty array of concepts.
   */
  public OntologyConcept[] concepts() {
    return Arrays.copyOf(concepts, concepts.length);
  }

  /**
   * Returns the namespace prefixes that abbreviate IRIs, used to resolve a QName {@code
   * prefix:local} into a full IRI.
   *
   * @return An unmodifiable map of prefix to IRI, empty if none are set.
   */
  public Map<String, String> prefixes() {
    return prefixes;
  }

  /**
   * Compares this definition with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects have equal fields.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof OntologyDefinition)) {
      return false;
    }
    OntologyDefinition that = (OntologyDefinition) other;
    return Objects.equals(aiContext, that.aiContext)
        && Arrays.equals(requires, that.requires)
        && Arrays.equals(concepts, that.concepts)
        && prefixes.equals(that.prefixes);
  }

  /**
   * Returns the hash code of this definition.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    int result = Objects.hash(aiContext, prefixes);
    result = 31 * result + Arrays.hashCode(requires);
    result = 31 * result + Arrays.hashCode(concepts);
    return result;
  }

  /**
   * Returns a readable representation of this definition.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "OntologyDefinition{concepts=" + concepts.length + ", prefixes=" + prefixes + '}';
  }

  /** Builder for {@link OntologyDefinition}. */
  public static final class Builder {

    @Nullable private AIContext aiContext;
    private String[] requires = new String[0];
    private OntologyConcept[] concepts;
    private Map<String, String> prefixes = new HashMap<>();

    private Builder() {}

    /**
     * Sets the optional AI context.
     *
     * @param aiContext The AI context, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withAiContext(@Nullable AIContext aiContext) {
      this.aiContext = aiContext;
      return this;
    }

    /**
     * Sets the ANSI SQL expressions that constrain the population of the whole ontology.
     *
     * @param requires The expressions, or {@code null} to set none.
     * @return This builder.
     */
    public Builder withRequires(@Nullable String... requires) {
      this.requires = requires == null ? new String[0] : requires;
      return this;
    }

    /**
     * Sets the concepts of the ontology.
     *
     * @param concepts At least one concept.
     * @return This builder.
     */
    public Builder withConcepts(OntologyConcept... concepts) {
      this.concepts = concepts;
      return this;
    }

    /**
     * Sets the namespace prefixes that abbreviate IRIs.
     *
     * @param prefixes The prefix to IRI map, or {@code null} to set none.
     * @return This builder.
     */
    public Builder withPrefixes(@Nullable Map<String, String> prefixes) {
      this.prefixes = prefixes == null ? new HashMap<>() : new HashMap<>(prefixes);
      return this;
    }

    /**
     * Builds the immutable definition.
     *
     * @return The definition.
     * @throws IllegalArgumentException If no concept is given, a concept or relationship name is
     *     duplicated, a reference does not resolve inside the definition, or a prefix is invalid.
     */
    public OntologyDefinition build() {
      Preconditions.checkArgument(
          concepts != null && concepts.length > 0, "An ontology must declare at least one concept");

      Set<String> conceptNames = new HashSet<>();
      Set<String> relationshipNames = new HashSet<>();
      for (OntologyConcept concept : concepts) {
        Preconditions.checkArgument(concept != null, "Ontology declares a null concept");
        Preconditions.checkArgument(
            conceptNames.add(concept.concept()),
            "Concept '%s' is declared more than once",
            concept.concept());
      }

      for (OntologyConcept concept : concepts) {
        checkReferences(concept, conceptNames);
        OntologyRelationship[] relationships = concept.relationships();
        if (relationships == null) {
          continue;
        }
        for (OntologyRelationship relationship : relationships) {
          Preconditions.checkArgument(
              relationshipNames.add(relationship.name()),
              "Relationship '%s' is declared more than once",
              relationship.name());
        }
      }

      for (String expression : requires) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(expression), "Ontology declares a blank requires expression");
      }
      for (Map.Entry<String, String> prefix : prefixes.entrySet()) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(prefix.getKey()), "Ontology declares a blank namespace prefix");
        Preconditions.checkArgument(
            StringUtils.isNotBlank(prefix.getValue()),
            "Ontology declares a blank IRI for prefix '%s'",
            prefix.getKey());
      }
      return new OntologyDefinition(this);
    }

    private static void checkReferences(OntologyConcept concept, Set<String> conceptNames) {
      String[] extendsConcepts = concept.extendsConcepts();
      if (extendsConcepts != null) {
        for (String extended : extendsConcepts) {
          Preconditions.checkArgument(
              conceptNames.contains(extended),
              "Concept '%s' extends '%s', which the ontology does not declare",
              concept.concept(),
              extended);
        }
      }

      OntologyRelationship[] relationships = concept.relationships();
      if (relationships == null) {
        return;
      }
      for (OntologyRelationship relationship : relationships) {
        OntologyRole[] roles = relationship.roles();
        if (roles == null) {
          continue;
        }
        for (OntologyRole role : roles) {
          Preconditions.checkArgument(
              conceptNames.contains(role.concept()),
              "Relationship '%s' names concept '%s', which the ontology does not declare",
              relationship.name(),
              role.concept());
        }
      }
    }
  }
}
