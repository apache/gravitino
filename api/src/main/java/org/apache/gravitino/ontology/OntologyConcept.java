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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * An immutable component of an {@link Ontology}: one concept of an {@link ConceptType}, with the
 * relationships that are keyed primarily by it.
 *
 * <p>A concept is identified by its name within the ontology. The optional IRI gives it a global
 * identity that outlives Gravitino, and relationship and term references point at the concept by
 * name, so renaming a concept breaks the references that use the old name.
 */
@Evolving
public final class OntologyConcept {

  private final String concept;
  private final ConceptType type;

  @Nullable private final String description;
  @Nullable private final String[] extendsConcepts;
  @Nullable private final String[] derivedBy;
  @Nullable private final String[] identifyBy;
  @Nullable private final String[] requires;
  @Nullable private final OntologyRelationship[] relationships;
  @Nullable private final String iri;

  private OntologyConcept(Builder builder) {
    this.concept = builder.concept;
    this.type = builder.type;
    this.description = builder.description;
    this.extendsConcepts = copyOrNull(builder.extendsConcepts);
    this.derivedBy = copyOrNull(builder.derivedBy);
    this.identifyBy = copyOrNull(builder.identifyBy);
    this.requires = copyOrNull(builder.requires);
    this.relationships = copyOrNull(builder.relationships);
    this.iri = builder.iri;
  }

  /**
   * Creates a builder for an immutable {@link OntologyConcept}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the concept name.
   *
   * @return The concept name.
   */
  public String concept() {
    return concept;
  }

  /**
   * Returns the concept type.
   *
   * @return The concept type.
   */
  public ConceptType type() {
    return type;
  }

  /**
   * Returns the concept description.
   *
   * @return The description, or {@code null} if it is not set.
   */
  @Nullable
  public String description() {
    return description;
  }

  /**
   * Returns a copy of the names of the concepts this concept extends.
   *
   * @return The extended concept names, or {@code null} if the concept extends nothing.
   */
  @Nullable
  public String[] extendsConcepts() {
    return copyOrNull(extendsConcepts);
  }

  /**
   * Returns a copy of the ANSI SQL expressions that derive this concept.
   *
   * @return The expressions, or {@code null} if they are not set.
   */
  @Nullable
  public String[] derivedBy() {
    return copyOrNull(derivedBy);
  }

  /**
   * Returns a copy of the names of the relationships to use as the preferred identifier of this
   * concept.
   *
   * @return The identifying relationship names, or {@code null} if they are not set.
   */
  @Nullable
  public String[] identifyBy() {
    return copyOrNull(identifyBy);
  }

  /**
   * Returns a copy of the ANSI SQL expressions that constrain this concept's population.
   *
   * @return The expressions, or {@code null} if they are not set.
   */
  @Nullable
  public String[] requires() {
    return copyOrNull(requires);
  }

  /**
   * Returns a copy of the relationships keyed primarily by this concept.
   *
   * @return The relationships, or {@code null} if the concept has none.
   */
  @Nullable
  public OntologyRelationship[] relationships() {
    return copyOrNull(relationships);
  }

  /**
   * Returns the global identifier of this concept.
   *
   * @return A full IRI or a QName resolved against the ontology's prefixes, or {@code null} if it
   *     is not set.
   */
  @Nullable
  public String iri() {
    return iri;
  }

  /**
   * Compares this concept with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects have equal fields.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof OntologyConcept)) {
      return false;
    }
    OntologyConcept that = (OntologyConcept) other;
    return concept.equals(that.concept)
        && type == that.type
        && Objects.equals(description, that.description)
        && Arrays.equals(extendsConcepts, that.extendsConcepts)
        && Arrays.equals(derivedBy, that.derivedBy)
        && Arrays.equals(identifyBy, that.identifyBy)
        && Arrays.equals(requires, that.requires)
        && Arrays.equals(relationships, that.relationships)
        && Objects.equals(iri, that.iri);
  }

  /**
   * Returns the hash code of this concept.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    int result = Objects.hash(concept, type, description, iri);
    result = 31 * result + Arrays.hashCode(extendsConcepts);
    result = 31 * result + Arrays.hashCode(derivedBy);
    result = 31 * result + Arrays.hashCode(identifyBy);
    result = 31 * result + Arrays.hashCode(requires);
    result = 31 * result + Arrays.hashCode(relationships);
    return result;
  }

  /**
   * Returns a readable representation of this concept.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "OntologyConcept{concept='" + concept + "', type=" + type + '}';
  }

  @Nullable
  private static <T> T[] copyOrNull(@Nullable T[] values) {
    return values == null ? null : Arrays.copyOf(values, values.length);
  }

  /** Builder for {@link OntologyConcept}. */
  public static final class Builder {

    private String concept;
    private ConceptType type;

    @Nullable private String description;
    @Nullable private String[] extendsConcepts;
    @Nullable private String[] derivedBy;
    @Nullable private String[] identifyBy;
    @Nullable private String[] requires;
    @Nullable private OntologyRelationship[] relationships;
    @Nullable private String iri;

    private Builder() {}

    /**
     * Sets the concept name.
     *
     * @param concept The non-blank concept name.
     * @return This builder.
     */
    public Builder withConcept(String concept) {
      this.concept = concept;
      return this;
    }

    /**
     * Sets the concept type.
     *
     * @param type The concept type.
     * @return This builder.
     */
    public Builder withType(ConceptType type) {
      this.type = type;
      return this;
    }

    /**
     * Sets the optional concept description.
     *
     * @param description The description, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withDescription(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the optional names of the concepts this concept extends.
     *
     * @param extendsConcepts The extended concept names, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withExtendsConcepts(@Nullable String... extendsConcepts) {
      this.extendsConcepts = extendsConcepts;
      return this;
    }

    /**
     * Sets the optional ANSI SQL expressions that derive this concept.
     *
     * @param derivedBy The expressions, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withDerivedBy(@Nullable String... derivedBy) {
      this.derivedBy = derivedBy;
      return this;
    }

    /**
     * Sets the optional names of the relationships to use as the preferred identifier.
     *
     * @param identifyBy The identifying relationship names, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withIdentifyBy(@Nullable String... identifyBy) {
      this.identifyBy = identifyBy;
      return this;
    }

    /**
     * Sets the optional ANSI SQL expressions that constrain this concept's population.
     *
     * @param requires The expressions, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withRequires(@Nullable String... requires) {
      this.requires = requires;
      return this;
    }

    /**
     * Sets the optional relationships keyed primarily by this concept.
     *
     * @param relationships The relationships, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withRelationships(@Nullable OntologyRelationship... relationships) {
      this.relationships = relationships;
      return this;
    }

    /**
     * Sets the optional global identifier.
     *
     * @param iri A full IRI or a QName, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withIri(@Nullable String iri) {
      this.iri = iri;
      return this;
    }

    /**
     * Builds the immutable concept.
     *
     * @return The concept.
     * @throws IllegalArgumentException If the name is null or blank, the type is null, two
     *     relationships share a name, or an array contains a null or blank element.
     */
    public OntologyConcept build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(concept), "Concept name must not be null or blank");
      Preconditions.checkArgument(type != null, "Concept '%s' must have a type", concept);

      checkNoNullElements(extendsConcepts, "extends");
      checkNoNullElements(derivedBy, "derived_by");
      checkNoNullElements(identifyBy, "identify_by");
      checkNoNullElements(requires, "requires");
      checkNoNullElements(relationships, "relationships");

      if (relationships != null) {
        Set<String> names = new HashSet<>();
        for (OntologyRelationship relationship : relationships) {
          Preconditions.checkArgument(
              names.add(relationship.name()),
              "Concept '%s' declares relationship '%s' more than once",
              concept,
              relationship.name());
        }
      }
      return new OntologyConcept(this);
    }

    private void checkNoNullElements(@Nullable String[] values, String field) {
      if (values == null) {
        return;
      }
      for (String value : values) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(value),
            "Concept '%s' has a null or blank element in %s",
            concept,
            field);
      }
    }

    private void checkNoNullElements(@Nullable OntologyRelationship[] values, String field) {
      if (values == null) {
        return;
      }
      for (OntologyRelationship value : values) {
        Preconditions.checkArgument(
            value != null, "Concept '%s' has a null element in %s", concept, field);
      }
    }
  }
}
