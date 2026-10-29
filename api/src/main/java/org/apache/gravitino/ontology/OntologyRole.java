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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * A concept playing a role in an {@link OntologyRelationship}, together with an optional role name.
 *
 * <p>The name is needed only when the same concept plays more than one role in the same
 * relationship, for example a transfer between two accounts. Otherwise the concept alone identifies
 * the role.
 */
@Evolving
public final class OntologyRole {

  private final String concept;

  @Nullable private final String name;

  private OntologyRole(Builder builder) {
    this.concept = builder.concept;
    this.name = builder.name;
  }

  /**
   * Creates a builder for an immutable {@link OntologyRole}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the name of the concept playing this role.
   *
   * @return The concept name.
   */
  public String concept() {
    return concept;
  }

  /**
   * Returns the optional role name, used when the same concept plays several roles in one
   * relationship.
   *
   * @return The role name, or {@code null} if the concept alone identifies the role.
   */
  @Nullable
  public String name() {
    return name;
  }

  /**
   * Compares this role with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects have the same concept and role name.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof OntologyRole)) {
      return false;
    }
    OntologyRole that = (OntologyRole) other;
    return concept.equals(that.concept) && Objects.equals(name, that.name);
  }

  /**
   * Returns the hash code of this role.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(concept, name);
  }

  /**
   * Returns a readable representation of this role.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "OntologyRole{concept='" + concept + "', name='" + name + "'}";
  }

  /** Builder for {@link OntologyRole}. */
  public static final class Builder {

    private String concept;

    @Nullable private String name;

    private Builder() {}

    /**
     * Sets the name of the concept playing this role.
     *
     * @param concept The non-blank concept name.
     * @return This builder.
     */
    public Builder withConcept(String concept) {
      this.concept = concept;
      return this;
    }

    /**
     * Sets the optional role name.
     *
     * @param name The role name, or {@code null} to let the concept identify the role.
     * @return This builder.
     */
    public Builder withName(@Nullable String name) {
      this.name = name;
      return this;
    }

    /**
     * Builds the immutable role.
     *
     * @return The role.
     * @throws IllegalArgumentException If the concept is null or blank, or the name is set but
     *     blank.
     */
    public OntologyRole build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(concept), "Role concept must not be null or blank");
      Preconditions.checkArgument(
          name == null || StringUtils.isNotBlank(name), "Role name must not be blank when set");
      return new OntologyRole(this);
    }
  }
}
