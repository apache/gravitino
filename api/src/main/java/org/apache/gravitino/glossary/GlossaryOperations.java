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

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.LookupTooBroadException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;

/**
 * Interface for the glossary term operations of a metalake.
 *
 * <p>A term that the caller cannot see, for example a {@link GlossaryTerm.Status#DRAFT} term of
 * another user, is left out of lists, and getting it throws {@link NoSuchTermException}.
 *
 * <p>In Step 1, {@code VIEW_DEFINITION} permits reading approved and deprecated terms. {@code
 * CREATE_DEFINITION} includes view access and permits creating and maintaining terms owned by the
 * caller. {@code MANAGE_DEFINITIONS} includes create and view access and permits managing all
 * terms, including other owners' drafts. Ownership means the current owner, including group
 * members; ownership alone does not replace effective create access. Expand allowed grant
 * implications before applying explicit denies to each required capability. All reads and writes of
 * term definitions require effective {@code VIEW_DEFINITION}. Denying view hides terms from reads
 * and blocks writes even for an owner with create access. Denying create prevents creation even
 * with an allow on manage, but effective manage can still authorize edits. Denying manage removes
 * the all-term path without removing effective create access to own terms. These privileges are
 * granted on the metalake in Step 1. The metalake owner passes privilege checks, but remains
 * subject to the Step 2 workflow and no-self-approval rule.
 *
 * <p>The owner API permits the current term owner or metalake owner to transfer ownership
 * independently of glossary privileges.
 *
 * <p>The server publication policy controls whether a write takes effect immediately. The default
 * policy publishes in one request. A policy requiring approval returns an unpublished candidate.
 * Inspect {@link GlossaryTerm#publication()} to distinguish a saved draft, submission and
 * publication. A candidate keeps the legacy DRAFT status until publication.
 */
@Evolving
public interface GlossaryOperations {

  /**
   * Lists the names of the terms in the metalake that are visible to the caller.
   *
   * @return The names of the terms.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  String[] listTerms() throws NoSuchMetalakeException;

  /**
   * Lists the terms in the metalake that are visible to the caller, with details.
   *
   * @return The terms.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  GlossaryTerm[] listTermsInfo() throws NoSuchMetalakeException;

  /**
   * Gets a term by its name. The name is compared by its normalized key, so case and extra spaces
   * do not matter.
   *
   * @param name The name of the term.
   * @return The term.
   * @throws NoSuchTermException If the term does not exist or is not visible to the caller.
   */
  GlossaryTerm getTerm(String name) throws NoSuchTermException;

  /**
   * Creates a term. The creator owns the published identity. Without an explicit status the default
   * policy publishes an APPROVED term, and an approval policy submits a private candidate. With
   * approval enabled use DRAFT to save without submitting. Explicit APPROVED is rejected.
   *
   * <p>The caller needs effective {@code VIEW_DEFINITION}, effective {@code CREATE_DEFINITION}, and
   * read access to each requested domain ({@code USE_DOMAIN} for published domains, {@code
   * MANAGE_DOMAIN} for drafts) and referenced broader or related term. Returned domains and
   * relations follow the same visibility rules as reads.
   *
   * @param spec The content of the term.
   * @return The published term or candidate with its actual publication outcome.
   * @throws TermAlreadyExistsException If a term with the same name or the same source reference
   *     already exists.
   * @throws NoSuchTermException If a broader or related term does not exist or is not visible to
   *     the caller.
   * @throws NotFoundException If a requested domain is missing or is a draft hidden from the
   *     caller.
   * @throws ForbiddenException If the caller lacks the required creation or domain privilege.
   * @throws IllegalArgumentException If a synonym is the name of another term, or a relation is not
   *     allowed.
   */
  GlossaryTerm createTerm(TermSpec spec)
      throws TermAlreadyExistsException, NoSuchTermException, NotFoundException, ForbiddenException,
          IllegalArgumentException;

  /**
   * Alters a term atomically. Under an approval policy, effective edits propose a candidate and
   * ordinary reads keep returning the published term. The revision increases once only when the
   * final effective state changes; a no-op preserves both the revision and {@link
   * GlossaryTerm#revisionToken()}. This overload is unconditional and applies changes to the
   * current term, even if its name has been reused since the caller read it. Use {@link
   * #alterTerm(String, String, TermChange...)} to guard against stale edits. Setting APPROVED
   * explicitly is rejected when the policy requires approval.
   *
   * <p>The caller needs effective {@code VIEW_DEFINITION}, plus effective {@code
   * MANAGE_DEFINITIONS} or current ownership with effective {@code CREATE_DEFINITION}. Read access
   * to referenced terms and domains is also required. Returned domains and relations follow the
   * same visibility rules as reads.
   *
   * @param name The name of the term.
   * @param changes The changes to apply.
   * @return The published term or candidate resulting from the request.
   * @throws NoSuchTermException If the term, or a broader or related term in the changes, does not
   *     exist or is not visible to the caller.
   * @throws TermAlreadyExistsException If the term is renamed to the name of another term, or its
   *     source reference is set to the one of another term.
   * @throws NotFoundException If a domain to add or remove is missing or is a draft hidden from the
   *     caller.
   * @throws ForbiddenException If the caller can see the term but lacks the required edit or domain
   *     privilege.
   * @throws IllegalArgumentException If a change is not valid, for example a synonym that is the
   *     name of another term, or a broader term that would create a cycle or a chain longer than
   *     the depth limit.
   */
  GlossaryTerm alterTerm(String name, TermChange... changes)
      throws NoSuchTermException, TermAlreadyExistsException, NotFoundException, ForbiddenException,
          IllegalArgumentException;

  /**
   * Alters a term only if its identity and revision still match a required expected token.
   *
   * <p>Copy a nonblank token unchanged from {@link GlossaryTerm#revisionToken()}. For unconditional
   * editing, use {@link #alterTerm(String, TermChange...)} instead. After authorizing the caller,
   * the implementation locks the term and compares its metalake, term identity and revision with
   * the token before any write or no-op detection. A mismatch throws {@link
   * TermVersionConflictException} without changing data, the revision or successful-change events.
   * A same-name replacement must conflict even if its numeric revision equals the old term's
   * revision. A hidden or missing root throws {@link NoSuchTermException} before comparing the
   * token. A matching no-op preserves the revision and token.
   *
   * <p>Under an approval policy, a current candidate token continues editing that candidate and
   * preserves its pending content. A live token starts a proposal from the published definition.
   * Address an already-published term by its live name until a proposed rename is published. For a
   * term awaiting first publication use the current candidate name.
   *
   * <p>The authorization and change-validation rules of {@link #alterTerm(String, TermChange...)}
   * also apply. After a conflict, reload and reconcile the changes before explicitly resubmitting;
   * do not automatically replay a stale replacement list. Implementations that do not support
   * conditional editing must fail before writing rather than ignore the token. The default
   * implementation always throws before writing; a missing token must not silently turn a
   * conditional request into an unconditional edit.
   *
   * @param name The name of the term.
   * @param expectedRevision The required nonblank opaque token from the last read.
   * @param changes The changes to apply.
   * @return The published term or candidate resulting from the request.
   * @throws NoSuchTermException If the term or a referenced term is missing or hidden.
   * @throws TermAlreadyExistsException If a name or source reference conflicts with another term.
   * @throws NotFoundException If a domain to add or remove is missing or is a draft hidden from the
   *     caller.
   * @throws ForbiddenException If the caller lacks edit or required domain access.
   * @throws IllegalArgumentException If a supported implementation receives a missing/malformed
   *     token or an invalid change.
   * @throws TermVersionConflictException If the expected identity or revision no longer matches.
   * @throws UnsupportedOperationException If conditional editing is not supported.
   */
  default GlossaryTerm alterTerm(String name, String expectedRevision, TermChange... changes)
      throws NoSuchTermException, TermAlreadyExistsException, NotFoundException, ForbiddenException,
          IllegalArgumentException, TermVersionConflictException {
    throw new UnsupportedOperationException("Conditional term editing is not supported.");
  }

  /**
   * Drops a term in any status, with its synonyms, relations, domain links and bindings. The caller
   * needs effective {@code VIEW_DEFINITION} and {@code MANAGE_DEFINITIONS}, regardless of ownership
   * or status. An approved term need not be deprecated first.
   *
   * <p>The checks run in this order:
   *
   * <ol>
   *   <li>If the term does not exist, or the caller cannot see it, nothing is dropped and the
   *       result is false. Both cases give the same result, so a hidden term stays hidden.
   *   <li>If the caller can see the term but lacks the required management privilege, a {@link
   *       ForbiddenException} is thrown. Owning the term with create access does not permit its
   *       deletion, even after changing its status.
   *   <li>Otherwise the term is dropped, whether draft, approved or deprecated.
   * </ol>
   *
   * @param name The name of the term.
   * @return True if the term was dropped, false if it does not exist or is not visible to the
   *     caller.
   * @throws ForbiddenException If the caller can see the term but may not drop it.
   */
  boolean dropTerm(String name) throws ForbiddenException;

  /**
   * Looks up business words, such as the words of a question to an agent. Each word is looked up on
   * its own, and its normalized key is compared with the names and synonyms of the terms. Only
   * exact matches count, so {@code customers} does not find {@code Customer}. Only {@link
   * GlossaryTerm.Status#APPROVED} terms are returned, with {@link GlossaryTerm#matchedBy()} and
   * {@link GlossaryTerm#bindings()} set, and {@link GlossaryTerm#bindingsIncluded()} true. The
   * bindings only hold the objects and metrics that the caller can access, at most 20 per term; see
   * {@link GlossaryTerm#bindings()}. A term is returned once, even when it matches several words.
   *
   * <p>The approved-only restriction applies to roots and all nested relation names, even for an
   * owner or a caller with management access who can read drafts through ordinary operations.
   * Hidden domains are omitted. Term status and returned content reflect one consistent read
   * snapshot. No entry means no authorized approved exact match, not that the word has no business
   * meaning. A binding identifies a candidate asset or metric and grants no data access. Candidate
   * scanning is bounded: at most 1,000 binding candidates per term and 10,000 authorization
   * candidates across the request, including projected terms, relations and domains. If the budget
   * is exhausted before completeness or a 21st visible binding can be established, throw {@link
   * LookupTooBroadException} without partial results or hidden counts/identities. Call {@link
   * #matchTerms(String[], String, boolean)} with false to omit binding expansion; do not
   * automatically retry the same expensive request.
   *
   * @param words The words to look up, at most 20.
   * @param domain The name of a domain to limit the lookup to its terms and those of its
   *     subdomains, or null to look up all terms.
   * @return The matched terms. A word that matches no term has no entry.
   * @throws NotFoundException If the domain does not exist or is not visible to the caller.
   * @throws IllegalArgumentException If no word is given, a word is blank, or there are more words
   *     than 20 words.
   * @throws LookupTooBroadException If the lookup cannot complete within its work budget.
   */
  GlossaryTerm[] matchTerms(String[] words, @Nullable String domain)
      throws NotFoundException, IllegalArgumentException, LookupTooBroadException;

  /**
   * Looks up business words with optional binding expansion.
   *
   * <p>All matching, domain, authorization, approved-only and consistent-snapshot rules of {@link
   * #matchTerms(String[], String)} apply in both modes. With true, return the usual bounded binding
   * preview. With false, omit binding expansion and return terms whose {@link
   * GlossaryTerm#bindingsIncluded()} is false; their empty binding array and false truncation flag
   * are unrequested placeholders, not evidence that no bindings exist. Matching metadata remains
   * available in {@link GlossaryTerm#matchedBy()}.
   *
   * <p>Disabling bindings still enforces the overall lookup budget: term/relation/domain projection
   * alone may throw {@link LookupTooBroadException}. An implementation that does not support
   * definition-only lookup must fail rather than expand bindings and discard them. The default
   * implementation delegates true to the existing lookup and rejects false before querying.
   *
   * @param words The words to look up, at most 20.
   * @param domain The domain and its subdomains to search, or null to search all terms.
   * @param includeBindings Whether to expand the authorized binding preview.
   * @return The matched approved terms, with binding details only when requested.
   * @throws NotFoundException If the domain is missing or hidden.
   * @throws IllegalArgumentException If the words are missing, blank or exceed the word limit.
   * @throws LookupTooBroadException If the lookup cannot complete within its work budget.
   * @throws UnsupportedOperationException If definition-only lookup is requested but unsupported.
   */
  default GlossaryTerm[] matchTerms(
      String[] words, @Nullable String domain, boolean includeBindings)
      throws NotFoundException, IllegalArgumentException, LookupTooBroadException {
    if (includeBindings) {
      return matchTerms(words, domain);
    }
    throw new UnsupportedOperationException("Definition-only term lookup is not supported.");
  }
}
