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

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchTermException;

/**
 * A glossary term defines a business word, such as "Active Customer", in a metalake. A term has a
 * definition, synonyms, links to documentation, and an optional source reference. It can belong to
 * zero or more domains, have several broader terms and any number of related terms, and be bound to
 * the metadata objects and metrics behind it.
 *
 * <p>Names and synonyms are compared by a normalized key: spaces at both ends are removed, each run
 * of whitespace becomes one space, and the text is lower-cased with {@code Locale.ROOT}. So {@code
 * Churn}, {@code churn} and {@code " CHURN "} are the same name. A name is unique in a metalake,
 * and a synonym cannot be the name of another term or of its own term.
 *
 * <p>A {@code GlossaryTerm} is a snapshot. The relation lists hold the names of the terms that are
 * visible to the caller; hidden terms are left out.
 */
@Evolving
public interface GlossaryTerm extends Auditable {

  /** The status of a term. */
  enum Status {
    /** The term is being written. It is visible only to the users who can create terms. */
    DRAFT,
    /** The term can be used, including by agents. This is the default status of a new term. */
    APPROVED,
    /** The term is replaced. It stays readable, but lookups for agents leave it out. */
    DEPRECATED
  }

  /**
   * Returns the name of the term. The case is kept as the user typed it; spaces at both ends are
   * removed.
   *
   * @return The name of the term.
   */
  String name();

  /**
   * Returns the definition of the term.
   *
   * @return The definition of the term, or null if it is not set.
   */
  @Nullable
  String definition();

  /**
   * Returns the synonyms of the term.
   *
   * @return The synonyms of the term, or an empty array if it has none.
   */
  String[] synonyms();

  /**
   * Returns the status of the term.
   *
   * @return The status of the term.
   */
  Status status();

  /**
   * Returns the names of the domains the term belongs to.
   *
   * @return The names of the domains, or an empty array if the term belongs to no domain.
   */
  String[] domains();

  /**
   * Returns the names of the direct broader terms. "A has broader term B" means "A is a kind of B".
   *
   * @return The names of the broader terms, or an empty array if the term is a top-level term.
   */
  String[] broader();

  /**
   * Returns the names of the direct narrower terms, the terms that have this term as a broader
   * term. Narrower terms are derived from broader terms and cannot be set.
   *
   * @return The names of the narrower terms, or an empty array if there are none.
   */
  String[] narrower();

  /**
   * Returns the names of the related terms. A related link shows on both terms.
   *
   * @return The names of the related terms, or an empty array if there are none.
   */
  String[] related();

  /**
   * Returns the documentation links of the term.
   *
   * @return The links of the term, or an empty array if there are none.
   */
  TermLink[] links();

  /**
   * Returns where an imported term came from, in the form {@code <system>:<id>}, for example {@code
   * datahub:urn:li:glossaryTerm:Customer}. It is unique in a metalake.
   *
   * @return The source reference, or empty if the term was created in Gravitino.
   */
  Optional<String> sourceRef();

  /**
   * Returns the properties of the term.
   *
   * @return The properties of the term.
   */
  Map<String, String> properties();

  /**
   * Returns the revision of the term. A new term has revision 1. Each successful alter call
   * increases it by one, however many changes the call holds, and so does each bind or unbind call
   * that changes the bindings of the term. Owner and tag changes do not increase it. A client can
   * use it to see that a term changed after it read it. A revision is not an approved version.
   *
   * @return The revision of the term.
   */
  long version();

  /**
   * Returns the words that matched this term. It is set only on the terms returned by {@link
   * GlossaryOperations#matchTerms(String[], String)}.
   *
   * @return The matches of this term, or an empty array if the term was not returned by a lookup.
   */
  default TermMatch[] matchedBy() {
    return new TermMatch[0];
  }

  /**
   * Returns the bindings of the term. It is set only on the terms returned by {@link
   * GlossaryOperations#matchTerms(String[], String)}.
   *
   * <p>The server first leaves out the objects and metrics that the caller cannot access, the same
   * as {@link #listBindings(TermBinding.Kind)}. Then it keeps at most 20 of the remaining bindings;
   * see {@link #bindingsTruncated()}. Use {@link #listBindings(TermBinding.Kind)} for all of them.
   *
   * @return The bindings of the term, or an empty array if the term was not returned by a lookup.
   */
  default TermBinding[] bindings() {
    return new TermBinding[0];
  }

  /**
   * Returns whether {@link #bindings()} was cut to the limit. Only the bindings that the caller can
   * access count, so a binding to a hidden object never sets this flag.
   *
   * @return True if the caller can access more bindings of the term than {@link #bindings()}
   *     returns.
   */
  default boolean bindingsTruncated() {
    return false;
  }

  /**
   * Lists the bindings of the term. Only the objects and metrics the caller can access are
   * returned.
   *
   * @param kind The kind of bindings to list, or null to list both kinds.
   * @return The bindings of the term.
   * @throws NoSuchTermException If the term does not exist or is not visible to the caller.
   */
  TermBinding[] listBindings(@Nullable TermBinding.Kind kind) throws NoSuchTermException;
}
