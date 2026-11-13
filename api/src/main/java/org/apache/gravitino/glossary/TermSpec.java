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
package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * The content of a new glossary term, passed to {@link GlossaryOperations#createTerm(TermSpec)}.
 * Only the name is required. Build one with {@link #builder(String)}.
 */
@Evolving
public final class TermSpec {

  private final String name;
  @Nullable private final String definition;
  private final String[] synonyms;
  @Nullable private final GlossaryTerm.Status status;
  private final String[] domains;
  private final String[] broader;
  private final String[] related;
  private final TermLink[] links;
  @Nullable private final String sourceRef;
  private final Map<String, String> properties;

  private TermSpec(Builder builder) {
    this.name = builder.name;
    this.definition = builder.definition;
    this.synonyms = builder.synonyms;
    this.status = builder.status;
    this.domains = builder.domains;
    this.broader = builder.broader;
    this.related = builder.related;
    this.links = builder.links;
    this.sourceRef = builder.sourceRef;
    this.properties = builder.properties;
  }

  /**
   * Creates a builder for a term with the given name. The server removes spaces at both ends of the
   * name and of each synonym, and keeps the case.
   *
   * @param name The name of the term, at most 128 characters.
   * @return The builder.
   * @throws IllegalArgumentException If the name is blank.
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /**
   * Returns the name of the term.
   *
   * @return The name of the term.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the definition of the term.
   *
   * @return The definition, or null if it is not set.
   */
  @Nullable
  public String definition() {
    return definition;
  }

  /**
   * Returns the synonyms of the term.
   *
   * @return The synonyms, or an empty array.
   */
  public String[] synonyms() {
    return synonyms.clone();
  }

  /**
   * Returns the status of the term.
   *
   * @return The requested status, or empty to let the server choose its creation default. Step 1
   *     defaults to {@link GlossaryTerm.Status#APPROVED} without review. Step 2 defaults to {@link
   *     GlossaryTerm.Status#DRAFT} and rejects another initial status. Omission is preserved in
   *     this value rather than replaced by a client-side default.
   */
  public Optional<GlossaryTerm.Status> status() {
    return Optional.ofNullable(status);
  }

  /**
   * Returns the names of the domains the term belongs to.
   *
   * @return The names of the domains, or an empty array.
   */
  public String[] domains() {
    return domains.clone();
  }

  /**
   * Returns the names of the broader terms.
   *
   * @return The names of the broader terms, or an empty array.
   */
  public String[] broader() {
    return broader.clone();
  }

  /**
   * Returns the names of the related terms.
   *
   * @return The names of the related terms, or an empty array.
   */
  public String[] related() {
    return related.clone();
  }

  /**
   * Returns the documentation links of the term.
   *
   * @return The links, or an empty array.
   */
  public TermLink[] links() {
    return links.clone();
  }

  /**
   * Returns the source reference of the term.
   *
   * @return The source reference, or empty if it is not set.
   */
  public Optional<String> sourceRef() {
    return Optional.ofNullable(sourceRef);
  }

  /**
   * Returns the properties of the term.
   *
   * @return The properties, or an empty map.
   */
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TermSpec)) {
      return false;
    }
    TermSpec that = (TermSpec) o;
    return Objects.equals(name, that.name)
        && Objects.equals(definition, that.definition)
        && Arrays.equals(synonyms, that.synonyms)
        && status == that.status
        && Arrays.equals(domains, that.domains)
        && Arrays.equals(broader, that.broader)
        && Arrays.equals(related, that.related)
        && Arrays.equals(links, that.links)
        && Objects.equals(sourceRef, that.sourceRef)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name, definition, status, sourceRef, properties);
    result = 31 * result + Arrays.hashCode(synonyms);
    result = 31 * result + Arrays.hashCode(domains);
    result = 31 * result + Arrays.hashCode(broader);
    result = 31 * result + Arrays.hashCode(related);
    result = 31 * result + Arrays.hashCode(links);
    return result;
  }

  @Override
  public String toString() {
    return "TermSpec{name="
        + name
        + ", status="
        + status
        + ", synonyms="
        + Arrays.toString(synonyms)
        + ", domains="
        + Arrays.toString(domains)
        + ", broader="
        + Arrays.toString(broader)
        + ", related="
        + Arrays.toString(related)
        + ", sourceRef="
        + sourceRef
        + "}";
  }

  /** A builder of {@link TermSpec}. */
  public static final class Builder {
    private final String name;
    @Nullable private String definition;
    private String[] synonyms = new String[0];
    @Nullable private GlossaryTerm.Status status;
    private String[] domains = new String[0];
    private String[] broader = new String[0];
    private String[] related = new String[0];
    private TermLink[] links = new TermLink[0];
    @Nullable private String sourceRef;
    private Map<String, String> properties = ImmutableMap.of();

    private Builder(String name) {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "Term name cannot be blank");
      this.name = name;
    }

    /**
     * Sets the definition of the term.
     *
     * @param definition The definition, or null.
     * @return This builder.
     */
    public Builder withDefinition(@Nullable String definition) {
      this.definition = definition;
      return this;
    }

    /**
     * Sets the synonyms of the term.
     *
     * @param synonyms The synonyms.
     * @return This builder.
     */
    public Builder withSynonyms(String... synonyms) {
      this.synonyms = copyNames(synonyms, "Synonym");
      return this;
    }

    /**
     * Sets the requested initial status. A null value preserves omission so that the server chooses
     * its default as described by {@link TermSpec#status()}.
     *
     * @param status The requested status, or null to use the server default.
     * @return This builder.
     */
    public Builder withStatus(@Nullable GlossaryTerm.Status status) {
      this.status = status;
      return this;
    }

    /**
     * Sets the names of the domains the term belongs to.
     *
     * @param domains The names of the domains.
     * @return This builder.
     */
    public Builder withDomains(String... domains) {
      this.domains = copyNames(domains, "Domain name");
      return this;
    }

    /**
     * Sets the names of the broader terms.
     *
     * @param broader The names of the broader terms.
     * @return This builder.
     */
    public Builder withBroader(String... broader) {
      this.broader = copyNames(broader, "Broader term name");
      return this;
    }

    /**
     * Sets the names of the related terms.
     *
     * @param related The names of the related terms.
     * @return This builder.
     */
    public Builder withRelated(String... related) {
      this.related = copyNames(related, "Related term name");
      return this;
    }

    /**
     * Sets the documentation links of the term.
     *
     * @param links The links.
     * @return This builder.
     */
    public Builder withLinks(TermLink... links) {
      Preconditions.checkArgument(links != null, "Links cannot be null");
      Preconditions.checkArgument(
          Arrays.stream(links).allMatch(Objects::nonNull), "A link cannot be null");
      this.links = links.clone();
      return this;
    }

    /**
     * Sets the source reference of the term, in the form {@code <system>:<id>}.
     *
     * @param sourceRef The source reference, or null.
     * @return This builder.
     */
    public Builder withSourceRef(@Nullable String sourceRef) {
      this.sourceRef = sourceRef;
      return this;
    }

    /**
     * Sets the properties of the term.
     *
     * @param properties The properties, or null for none.
     * @return This builder.
     */
    public Builder withProperties(@Nullable Map<String, String> properties) {
      this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
      return this;
    }

    /**
     * Builds the term content.
     *
     * @return The term content.
     */
    public TermSpec build() {
      return new TermSpec(this);
    }

    private static String[] copyNames(String[] names, String what) {
      Preconditions.checkArgument(names != null, "%s list cannot be null", what);
      for (String name : names) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "%s cannot be blank", what);
      }
      return names.clone();
    }
  }
}
