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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * An immutable relationship between concepts in an {@link Ontology}.
 *
 * <p>{@link #verbalizes()} is required and must hold at least one natural-language phrasing. The
 * Apache Ossie ontology specification makes it a required field because a relationship an agent
 * cannot phrase is a relationship it cannot use to answer a question; Gravitino rejects such a
 * relationship rather than defaulting it.
 */
@Evolving
public final class OntologyRelationship {

  private final String name;
  private final String[] verbalizes;

  @Nullable private final String description;
  @Nullable private final OntologyRole[] roles;
  @Nullable private final Multiplicity multiplicity;
  @Nullable private final String[] derivedBy;
  @Nullable private final String[] requires;
  @Nullable private final String iri;

  private OntologyRelationship(Builder builder) {
    this.name = builder.name;
    this.verbalizes = Arrays.copyOf(builder.verbalizes, builder.verbalizes.length);
    this.description = builder.description;
    this.roles = copyOrNull(builder.roles);
    this.multiplicity = builder.multiplicity;
    this.derivedBy = copyOrNull(builder.derivedBy);
    this.requires = copyOrNull(builder.requires);
    this.iri = builder.iri;
  }

  /**
   * Creates a builder for an immutable {@link OntologyRelationship}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the relationship name.
   *
   * @return The relationship name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns a copy of the natural-language phrasings of this relationship.
   *
   * @return The non-empty array of phrasings.
   */
  public String[] verbalizes() {
    return Arrays.copyOf(verbalizes, verbalizes.length);
  }

  /**
   * Returns the relationship description.
   *
   * @return The description, or {@code null} if it is not set.
   */
  @Nullable
  public String description() {
    return description;
  }

  /**
   * Returns a copy of the roles of this relationship.
   *
   * @return The roles, or {@code null} if they are not set.
   */
  @Nullable
  public OntologyRole[] roles() {
    return copyOrNull(roles);
  }

  /**
   * Returns the cardinality of this relationship.
   *
   * @return The multiplicity, or {@code null} if the relationship is unconstrained.
   */
  @Nullable
  public Multiplicity multiplicity() {
    return multiplicity;
  }

  /**
   * Returns a copy of the ANSI SQL expressions that derive this relationship.
   *
   * @return The expressions, or {@code null} if they are not set.
   */
  @Nullable
  public String[] derivedBy() {
    return copyOrNull(derivedBy);
  }

  /**
   * Returns a copy of the ANSI SQL expressions that constrain this relationship's population.
   *
   * @return The expressions, or {@code null} if they are not set.
   */
  @Nullable
  public String[] requires() {
    return copyOrNull(requires);
  }

  /**
   * Returns the global identifier of this relationship.
   *
   * @return A full IRI or a QName resolved against the ontology's prefixes, or {@code null} if it
   *     is not set.
   */
  @Nullable
  public String iri() {
    return iri;
  }

  /**
   * Compares this relationship with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects have equal fields.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof OntologyRelationship)) {
      return false;
    }
    OntologyRelationship that = (OntologyRelationship) other;
    return name.equals(that.name)
        && Arrays.equals(verbalizes, that.verbalizes)
        && Objects.equals(description, that.description)
        && Arrays.equals(roles, that.roles)
        && multiplicity == that.multiplicity
        && Arrays.equals(derivedBy, that.derivedBy)
        && Arrays.equals(requires, that.requires)
        && Objects.equals(iri, that.iri);
  }

  /**
   * Returns the hash code of this relationship.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    int result = Objects.hash(name, description, multiplicity, iri);
    result = 31 * result + Arrays.hashCode(verbalizes);
    result = 31 * result + Arrays.hashCode(roles);
    result = 31 * result + Arrays.hashCode(derivedBy);
    result = 31 * result + Arrays.hashCode(requires);
    return result;
  }

  /**
   * Returns a readable representation of this relationship.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "OntologyRelationship{name='"
        + name
        + "', verbalizes="
        + Arrays.toString(verbalizes)
        + ", multiplicity="
        + multiplicity
        + '}';
  }

  @Nullable
  private static <T> T[] copyOrNull(@Nullable T[] values) {
    return values == null ? null : Arrays.copyOf(values, values.length);
  }

  /** Builder for {@link OntologyRelationship}. */
  public static final class Builder {

    private String name;
    private String[] verbalizes;

    @Nullable private String description;
    @Nullable private OntologyRole[] roles;
    @Nullable private Multiplicity multiplicity;
    @Nullable private String[] derivedBy;
    @Nullable private String[] requires;
    @Nullable private String iri;

    private Builder() {}

    /**
     * Sets the relationship name.
     *
     * @param name The non-blank relationship name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the natural-language phrasings of this relationship.
     *
     * @param verbalizes At least one non-blank phrasing.
     * @return This builder.
     */
    public Builder withVerbalizes(String... verbalizes) {
      this.verbalizes = verbalizes;
      return this;
    }

    /**
     * Sets the optional relationship description.
     *
     * @param description The description, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withDescription(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the optional roles of this relationship.
     *
     * @param roles The roles, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withRoles(@Nullable OntologyRole... roles) {
      this.roles = roles;
      return this;
    }

    /**
     * Sets the optional multiplicity.
     *
     * @param multiplicity The multiplicity, or {@code null} to leave the relationship
     *     unconstrained.
     * @return This builder.
     */
    public Builder withMultiplicity(@Nullable Multiplicity multiplicity) {
      this.multiplicity = multiplicity;
      return this;
    }

    /**
     * Sets the optional ANSI SQL expressions that derive this relationship.
     *
     * @param derivedBy The expressions, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withDerivedBy(@Nullable String... derivedBy) {
      this.derivedBy = derivedBy;
      return this;
    }

    /**
     * Sets the optional ANSI SQL expressions that constrain this relationship's population.
     *
     * @param requires The expressions, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withRequires(@Nullable String... requires) {
      this.requires = requires;
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
     * Builds the immutable relationship.
     *
     * @return The relationship.
     * @throws IllegalArgumentException If the name is null or blank, no phrasing is given, a
     *     phrasing is blank, or an array contains a null element.
     */
    public OntologyRelationship build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(name), "Relationship name must not be null or blank");
      Preconditions.checkArgument(
          verbalizes != null && verbalizes.length > 0,
          "Relationship '%s' must have at least one verbalization",
          name);
      for (String phrasing : verbalizes) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(phrasing),
            "Relationship '%s' has a null or blank verbalization",
            name);
      }
      checkNoNullElements(roles, "roles", name);
      checkNoNullElements(derivedBy, "derived_by", name);
      checkNoNullElements(requires, "requires", name);
      return new OntologyRelationship(this);
    }

    private static void checkNoNullElements(
        @Nullable Object[] values, String field, String relationshipName) {
      if (values == null) {
        return;
      }
      for (Object value : values) {
        Preconditions.checkArgument(
            value != null, "Relationship '%s' has a null element in %s", relationshipName, field);
      }
    }
  }
}
