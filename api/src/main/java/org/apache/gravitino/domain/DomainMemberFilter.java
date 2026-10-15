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
package org.apache.gravitino.domain;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

/**
 * The filter of {@link Domain#listMembers(DomainMemberFilter)}.
 *
 * <p>The members of the domain itself are always listed. The sub-domain options choose which
 * sub-domains' members are listed as well:
 *
 * <ul>
 *   <li>All sub-domains, the default: {@link Builder#withIncludeAllSubdomains(boolean)
 *       withIncludeAllSubdomains(true)}.
 *   <li>Only the given sub-domains: {@link Builder#withSubdomains(String...)}.
 *   <li>No sub-domain: {@code withIncludeAllSubdomains(false)} without sub-domains.
 * </ul>
 *
 * <p>To list only the members of a sub-domain, call {@link Domain#listMembers(DomainMemberFilter)}
 * on the sub-domain.
 */
@Evolving
public final class DomainMemberFilter {

  private final boolean includeAllSubdomains;
  private final Set<MetadataObject.Type> types;
  @Nullable private final String nameContains;
  private final Set<String> subdomains;

  private DomainMemberFilter(Builder builder) {
    this.includeAllSubdomains = builder.includeAllSubdomains;
    this.types = builder.types;
    this.nameContains = builder.nameContains;
    this.subdomains = builder.subdomains;
  }

  /**
   * Creates a builder for a domain member filter.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns whether the members of all sub-domains are listed. When it is true, {@link
   * #subdomains()} is empty.
   *
   * @return True if the members of all sub-domains are listed, the default.
   */
  public boolean includeAllSubdomains() {
    return includeAllSubdomains;
  }

  /**
   * Returns the metadata object types of the members to list.
   *
   * @return The metadata object types, or an empty set to list members of all {@link
   *     DomainMember#SUPPORTED_TYPES supported types}.
   */
  public Set<MetadataObject.Type> types() {
    return types;
  }

  /**
   * Returns the text the full name of a member must contain, ignoring case.
   *
   * @return The text, or empty to match any name.
   */
  public Optional<String> nameContains() {
    return Optional.ofNullable(nameContains);
  }

  /**
   * Returns the sub-domains whose members are listed in addition to those of the domain itself. It
   * is used only when {@link #includeAllSubdomains()} is false.
   *
   * @return The names of the sub-domains, or an empty set when all sub-domains or no sub-domain are
   *     listed.
   */
  public Set<String> subdomains() {
    return subdomains;
  }

  /**
   * Compares this filter with another object.
   *
   * @param other The object to compare.
   * @return {@code true} if the object is a filter with the same fields.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DomainMemberFilter)) {
      return false;
    }
    DomainMemberFilter that = (DomainMemberFilter) other;
    return includeAllSubdomains == that.includeAllSubdomains
        && types.equals(that.types)
        && Objects.equals(nameContains, that.nameContains)
        && subdomains.equals(that.subdomains);
  }

  /**
   * Returns the hash code of this filter.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(includeAllSubdomains, types, nameContains, subdomains);
  }

  /**
   * Returns a string representation of this filter.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "DomainMemberFilter{"
        + "includeAllSubdomains="
        + includeAllSubdomains
        + ", types="
        + types
        + ", nameContains="
        + nameContains
        + ", subdomains="
        + subdomains
        + "}";
  }

  /** A builder for {@link DomainMemberFilter}. */
  public static final class Builder {

    private boolean includeAllSubdomains = true;
    private Set<MetadataObject.Type> types = ImmutableSet.of();
    @Nullable private String nameContains;
    private Set<String> subdomains = ImmutableSet.of();

    private Builder() {}

    /**
     * Sets whether the members of all sub-domains are listed. Defaults to true. Setting it to true
     * clears the sub-domains set by {@link #withSubdomains(String...)}; setting it to false without
     * sub-domains lists only the members of the domain itself.
     *
     * @param includeAllSubdomains Whether the members of all sub-domains are listed.
     * @return This builder.
     */
    public Builder withIncludeAllSubdomains(boolean includeAllSubdomains) {
      this.includeAllSubdomains = includeAllSubdomains;
      if (includeAllSubdomains) {
        this.subdomains = ImmutableSet.of();
      }
      return this;
    }

    /**
     * Sets the metadata object types of the members to list. Defaults to all {@link
     * DomainMember#SUPPORTED_TYPES supported types}.
     *
     * @param types The metadata object types.
     * @return This builder.
     * @throws IllegalArgumentException If the types are null, contain null, or contain a type that
     *     is not in {@link DomainMember#SUPPORTED_TYPES}.
     */
    public Builder withTypes(MetadataObject.Type... types) {
      Preconditions.checkArgument(types != null, "types must not be null");
      for (MetadataObject.Type type : types) {
        Preconditions.checkArgument(type != null, "types must not contain null");
        Preconditions.checkArgument(
            DomainMember.SUPPORTED_TYPES.contains(type),
            "Type %s cannot be a domain member, supported types are %s",
            type,
            DomainMember.SUPPORTED_TYPES);
      }
      this.types = ImmutableSet.copyOf(types);
      return this;
    }

    /**
     * Sets the text the full name of a member must contain, ignoring case.
     *
     * @param nameContains The text, or null to match any name.
     * @return This builder.
     * @throws IllegalArgumentException If the text is blank.
     */
    public Builder withNameContains(@Nullable String nameContains) {
      Preconditions.checkArgument(
          nameContains == null || StringUtils.isNotBlank(nameContains),
          "nameContains must not be blank");
      this.nameContains = nameContains;
      return this;
    }

    /**
     * Sets the sub-domains whose members are listed in addition to those of the domain itself. It
     * replaces {@link #withIncludeAllSubdomains(boolean) withIncludeAllSubdomains(true)}; without
     * names, only the members of the domain itself are listed.
     *
     * @param subdomains The names of the sub-domains.
     * @return This builder.
     * @throws IllegalArgumentException If the names are null, or contain a null or blank name.
     */
    public Builder withSubdomains(String... subdomains) {
      Preconditions.checkArgument(subdomains != null, "subdomains must not be null");
      for (String subdomain : subdomains) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(subdomain), "subdomains must not contain a blank name");
      }
      this.subdomains = ImmutableSet.copyOf(subdomains);
      this.includeAllSubdomains = false;
      return this;
    }

    /**
     * Builds a domain member filter.
     *
     * @return The immutable domain member filter.
     */
    public DomainMemberFilter build() {
      return new DomainMemberFilter(this);
    }
  }
}
