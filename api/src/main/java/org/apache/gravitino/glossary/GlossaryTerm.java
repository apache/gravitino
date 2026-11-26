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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermVersionConflictException;

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
 * visible to the caller; hidden terms are left out. All projections, including mutation responses,
 * omit domains the caller cannot read. Lookup results additionally omit non-approved relation
 * names, as specified by {@link GlossaryOperations#matchTerms(String[], String)}.
 */
@Evolving
public interface GlossaryTerm extends Auditable {

  /**
   * Identifies the returned business version and its publication outcome on a version-aware write
   * or history read. A submitted or saved candidate is private and uses DRAFT in the legacy status
   * field.
   *
   * @return the publication metadata, or null for an unversioned result
   */
  @Nullable
  default TermPublication publication() {
    return null;
  }

  /** The status of a term. */
  enum Status {
    /**
     * The term is being written. In Step 1, reading requires effective {@code VIEW_DEFINITION}. The
     * caller must also be the current owner with effective {@code CREATE_DEFINITION} (including
     * group members), or have effective {@code MANAGE_DEFINITIONS}. The shared deny rules in {@link
     * GlossaryOperations} apply; create access alone does not expose another owner's draft.
     */
    DRAFT,
    /**
     * The term is available to authorized readers and agents. This is the Step 1 creation default,
     * not evidence of human review. Step 2 defaults new terms to draft and requires approval.
     */
    APPROVED,
    /** The term is replaced. It stays readable, but lookups for agents leave it out. */
    DEPRECATED;

    /**
     * Parses a term status by name, ignoring case.
     *
     * @param value The status name to parse.
     * @return The matching term status.
     * @throws IllegalArgumentException If the value does not match a status name.
     */
    public static Status fromString(String value) {
      for (Status status : values()) {
        if (status.name().equalsIgnoreCase(value)) {
          return status;
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Invalid term status '%s', expected one of %s", value, Arrays.toString(values())));
    }
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
   * Returns the names of the domains the term belongs to that the caller can read. Hidden domain
   * names, IDs and counts are not exposed. Filtering does not change stored membership or the
   * revision, and a term can remain readable even if all its domains are hidden.
   *
   * @return The visible domain names, or an empty array if none are visible.
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
   * Returns the revision of the term. A new term has revision 1. Each alter whose final effective
   * state changes increases it by one, however many changes the call holds, and so does each bind
   * or unbind call that changes the bindings of the term. Owner and tag changes do not increase it.
   * A client can use it to see that a term changed after it read it. A revision is not an approved
   * version or a safe conditional-edit guard on its own: a same-name replacement starts again at
   * revision 1. Use {@link #revisionToken()} for conditional editing. A no-op preserves both
   * values, including repeated additions/removals and updates whose final effective state is
   * unchanged.
   *
   * <p>Cleanup also increases the revision of each surviving term that loses a broader parent,
   * related relation or binding, exactly once per cleanup transaction regardless of the number of
   * losses. Unaffected terms, no-op cleanup and changes only to a derived narrower list do not
   * increase it. In Step 2, pending changes do not alter the live revision or content; the previous
   * approved state remains available to readers and agents until approval.
   *
   * @return The revision of the term.
   */
  long version();

  /**
   * Returns an opaque token identifying this snapshot's metalake, term identity and revision.
   *
   * <p>Copy the token unchanged into {@link GlossaryOperations#alterTerm(String, String,
   * TermChange...)}. Do not parse it, construct it from the term's name or numeric revision, or use
   * it as an authorization credential. A replacement term has a different token even when its name
   * and numeric revision equal those of the old term. An effective change produces a new token; a
   * no-op does not. Owner and tag changes alone do not change it.
   *
   * @return The nonblank opaque identity-and-revision token.
   */
  String revisionToken();

  /**
   * Returns the words that matched this term. It is set only on terms returned by the {@link
   * GlossaryOperations#matchTerms(String[], String)} overloads, including definition-only lookup.
   *
   * @return The matches of this term, or an empty array if the term was not returned by a lookup.
   */
  default TermMatch[] matchedBy() {
    return new TermMatch[0];
  }

  /**
   * Returns whether a lookup requested the embedded binding preview.
   *
   * <p>This is true for {@link GlossaryOperations#matchTerms(String[], String)} and lookups with
   * binding expansion enabled, even if no authorized binding exists. It is false on ordinary reads
   * and definition-only lookups. When false, {@link #bindings()} and {@link #bindingsTruncated()}
   * return empty/false placeholders that must not be interpreted as a complete binding result. REST
   * projections omit both binding fields when they were not requested.
   *
   * @return Whether the embedded binding preview was requested.
   */
  boolean bindingsIncluded();

  /**
   * Returns the embedded bindings of the term, when {@link #bindingsIncluded()} is true.
   *
   * <p>The server first leaves out the objects and metrics that the caller cannot access, the same
   * as {@link #listBindings(TermBinding.Kind)}. Then it keeps at most 20 of the remaining bindings;
   * see {@link #bindingsTruncated()}. Use {@link #listBindings(TermBinding.Kind)} to begin a paged
   * read of all bindings, following each continuation until it is absent.
   *
   * @return The authorized preview, or an empty array when none are visible or not requested; check
   *     {@link #bindingsIncluded()} to distinguish those cases.
   */
  default TermBinding[] bindings() {
    return new TermBinding[0];
  }

  /**
   * Returns whether {@link #bindings()} was cut to the limit. Only the bindings that the caller can
   * access count, so a binding to a hidden object never sets this flag. It is false when binding
   * details were not requested; check {@link #bindingsIncluded()} first.
   *
   * @return True if a requested preview omitted further authorized bindings; false when complete or
   *     not requested.
   */
  default boolean bindingsTruncated() {
    return false;
  }

  /**
   * Returns the first page of bindings using {@link TermBindingPage#DEFAULT_PAGE_SIZE}.
   *
   * <p>The same page type is returned even for an empty first page, preserving any continuation.
   * This method does not automatically read subsequent pages.
   *
   * @param kind The kind of bindings to list, or null to list both kinds.
   * @return The first page of authorized bindings and its continuation, if any.
   * @throws NoSuchTermException If the term does not exist or is not visible to the caller.
   */
  default TermBindingPage listBindings(@Nullable TermBinding.Kind kind) throws NoSuchTermException {
    return listBindings(kind, TermBindingPage.DEFAULT_PAGE_SIZE, null);
  }

  /**
   * Returns one bounded page of authorized bindings of the current term.
   *
   * <p>Each call examines at most 1,000 candidates and returns at most {@code pageSize} visible
   * bindings. Hidden candidates advance the cursor, so an empty page can still have a continuation.
   * Only an absent continuation proves that scanning is complete. Do not silently drain all pages
   * or discard the token. Every page rechecks term visibility and target access; paging does not
   * freeze authorization.
   *
   * <p>The opaque, tamper-resistant cursor binds the metalake, term identity, revision, filters and
   * scan position without revealing hidden target IDs/counts. A changed identity or revision throws
   * {@link TermVersionConflictException}; restart explicitly from the first page without combining
   * old and new pages. A malformed cursor or changed filters throws {@link
   * IllegalArgumentException}. Missing/hidden roots throw {@link NoSuchTermException} before cursor
   * comparison. An implementation must not silently ignore a cursor or page size.
   *
   * @param kind The binding kind, or null for both kinds; retain it on subsequent pages.
   * @param pageSize The maximum visible results, from 1 to {@link TermBindingPage#MAX_PAGE_SIZE}.
   * @param pageToken The previous page's continuation, or null to start a new scan.
   * @return One page of visible bindings, possibly empty with a continuation.
   * @throws NoSuchTermException If the term is missing or hidden.
   * @throws IllegalArgumentException If the page size is invalid, the cursor is malformed or the
   *     filters changed.
   * @throws TermVersionConflictException If the cursor's term identity or revision changed.
   */
  TermBindingPage listBindings(
      @Nullable TermBinding.Kind kind, int pageSize, @Nullable String pageToken)
      throws NoSuchTermException, IllegalArgumentException, TermVersionConflictException;
}
