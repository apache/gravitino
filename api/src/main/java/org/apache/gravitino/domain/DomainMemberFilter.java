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
 * The filter of {@link Domain#listMembers(DomainMemberFilter)}. A filter built without setting any
 * field matches all members of the domain and its sub-domains.
 */
@Evolving
public final class DomainMemberFilter {

  private final boolean includeSubdomains;
  private final Set<MetadataObject.Type> types;
  @Nullable private final String nameContains;
  @Nullable private final String subdomain;

  private DomainMemberFilter(Builder builder) {
    this.includeSubdomains = builder.includeSubdomains;
    this.types = builder.types;
    this.nameContains = builder.nameContains;
    this.subdomain = builder.subdomain;
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
   * Returns whether the members of the sub-domains are included.
   *
   * @return True if the members of the sub-domains are included, the default.
   */
  public boolean includeSubdomains() {
    return includeSubdomains;
  }

  /**
   * Returns the metadata object types of the members to list.
   *
   * @return The metadata object types, or an empty set to list members of all types.
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
   * Returns the sub-domain whose members are listed.
   *
   * @return The name of the sub-domain, or empty to list the members of the domain and all its
   *     sub-domains.
   */
  public Optional<String> subdomain() {
    return Optional.ofNullable(subdomain);
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
    return includeSubdomains == that.includeSubdomains
        && types.equals(that.types)
        && Objects.equals(nameContains, that.nameContains)
        && Objects.equals(subdomain, that.subdomain);
  }

  /**
   * Returns the hash code of this filter.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(includeSubdomains, types, nameContains, subdomain);
  }

  /**
   * Returns a string representation of this filter.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "DomainMemberFilter{"
        + "includeSubdomains="
        + includeSubdomains
        + ", types="
        + types
        + ", nameContains="
        + nameContains
        + ", subdomain="
        + subdomain
        + "}";
  }

  /** A builder for {@link DomainMemberFilter}. */
  public static final class Builder {

    private boolean includeSubdomains = true;
    private Set<MetadataObject.Type> types = ImmutableSet.of();
    @Nullable private String nameContains;
    @Nullable private String subdomain;

    private Builder() {}

    /**
     * Sets whether the members of the sub-domains are included. Defaults to true.
     *
     * @param includeSubdomains Whether the members of the sub-domains are included.
     * @return This builder.
     */
    public Builder withIncludeSubdomains(boolean includeSubdomains) {
      this.includeSubdomains = includeSubdomains;
      return this;
    }

    /**
     * Sets the metadata object types of the members to list. Defaults to all types.
     *
     * @param types The metadata object types.
     * @return This builder.
     * @throws IllegalArgumentException If the types are null, contain null, or contain {@link
     *     MetadataObject.Type#DOMAIN}, which is never a member.
     */
    public Builder withTypes(MetadataObject.Type... types) {
      Preconditions.checkArgument(types != null, "types must not be null");
      for (MetadataObject.Type type : types) {
        Preconditions.checkArgument(type != null, "types must not contain null");
        Preconditions.checkArgument(
            type != MetadataObject.Type.DOMAIN, "A domain cannot be a member of a domain");
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
     * Sets the sub-domain whose members are listed.
     *
     * @param subdomain The name of the sub-domain, or null to list the members of the domain and
     *     all its sub-domains.
     * @return This builder.
     * @throws IllegalArgumentException If the name is blank.
     */
    public Builder withSubdomain(@Nullable String subdomain) {
      Preconditions.checkArgument(
          subdomain == null || StringUtils.isNotBlank(subdomain), "subdomain must not be blank");
      this.subdomain = subdomain;
      return this;
    }

    /**
     * Builds a domain member filter.
     *
     * @return The immutable domain member filter.
     * @throws IllegalArgumentException If a sub-domain is set while the members of the sub-domains
     *     are excluded.
     */
    public DomainMemberFilter build() {
      Preconditions.checkArgument(
          subdomain == null || includeSubdomains,
          "subdomain cannot be set when includeSubdomains is false");
      return new DomainMemberFilter(this);
    }
  }
}
