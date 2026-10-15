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
 * The filter of {@link Domain#listMembers(DomainMemberFilter)}. Each option narrows the listed
 * members, and an option that is not set does not narrow them, so a filter built without setting
 * any option lists the whole scope of the domain.
 *
 * <p>The scope of a top-level domain is its own members plus the members of its sub-domains visible
 * to the caller; the scope of a sub-domain is its own members. The domains option selects the
 * scopes to list: each name is the listed domain itself or one of its sub-domains, and the members
 * in the union of their scopes are listed. For example, on domain "Finance" with sub-domains "Tax"
 * and "Trading":
 *
 * <ul>
 *   <li>not set, or {@code withDomains("Finance")}: the members of Finance, Tax and Trading;
 *   <li>{@code withDomains("Tax")}: only the members of Tax;
 *   <li>{@code withDomains("Tax", "Trading")}: the members of Tax and Trading.
 * </ul>
 *
 * <p>With the whole scope selected, the default, the members of the listed domain itself are the
 * listed members whose {@link DomainMember#subdomain()} is empty; select them to keep only those.
 */
@Evolving
public final class DomainMemberFilter {

  private final Set<MetadataObject.Type> types;
  @Nullable private final String nameContains;
  private final Set<String> domains;

  private DomainMemberFilter(Builder builder) {
    this.types = builder.types;
    this.nameContains = builder.nameContains;
    this.domains = builder.domains;
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
   * Returns the domains whose scopes are listed: the listed domain itself, its sub-domains, or
   * both.
   *
   * @return The names of the domains, or an empty set to list the whole scope of the listed domain.
   */
  public Set<String> domains() {
    return domains;
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
    return types.equals(that.types)
        && Objects.equals(nameContains, that.nameContains)
        && domains.equals(that.domains);
  }

  /**
   * Returns the hash code of this filter.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(types, nameContains, domains);
  }

  /**
   * Returns a string representation of this filter.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "DomainMemberFilter{"
        + "types="
        + types
        + ", nameContains="
        + nameContains
        + ", domains="
        + domains
        + "}";
  }

  /** A builder for {@link DomainMemberFilter}. */
  public static final class Builder {

    private Set<MetadataObject.Type> types = ImmutableSet.of();
    @Nullable private String nameContains;
    private Set<String> domains = ImmutableSet.of();

    private Builder() {}

    /**
     * Sets the metadata object types of the members to list. Defaults to all {@link
     * DomainMember#SUPPORTED_TYPES supported types}.
     *
     * @param types The metadata object types, or none to list members of all supported types.
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
     * Sets the domains whose scopes are listed. Each name is the listed domain itself, whose scope
     * includes its visible sub-domains, or one of its sub-domains. Names that are neither, or that
     * are not visible to the caller, are ignored when the members are listed; if all names are
     * ignored, no member is listed. Defaults to the whole scope of the listed domain.
     *
     * @param domains The names of the domains, or none to list the whole scope of the listed
     *     domain.
     * @return This builder.
     * @throws IllegalArgumentException If the names are null, or contain a null or blank name.
     */
    public Builder withDomains(String... domains) {
      Preconditions.checkArgument(domains != null, "domains must not be null");
      for (String domain : domains) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(domain), "domains must not contain a blank name");
      }
      this.domains = ImmutableSet.copyOf(domains);
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
