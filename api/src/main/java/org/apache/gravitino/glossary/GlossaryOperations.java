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
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;

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
 * implications before applying explicit denies to each required capability. Every term read and
 * write requires effective {@code VIEW_DEFINITION}; denying view hides terms from reads and blocks
 * writes even for an owner with create access. Denying create prevents creation even with an allow
 * on manage, but effective manage can still authorize edits. Denying manage removes the all-term
 * path without removing effective create access to own terms. These privileges are granted on the
 * metalake in Step 1. The metalake owner passes privilege checks, but remains subject to the Step 2
 * workflow and no-self-approval rule.
 *
 * <p>The write methods describe Step 1: changes take effect immediately, without review. Step 2
 * adds scoped management and review: new terms default to draft, and another authorized manage user
 * must approve a submitted change. Neither management privileges nor a direct status update
 * bypasses that workflow. Proposal and decision APIs are separate from these immediate-write
 * operations.
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
   * Creates a term in Step 1. The caller becomes its owner. Without a status, the server uses
   * {@link GlossaryTerm.Status#APPROVED}; this does not mean that a human reviewed the term.
   *
   * <p>The caller needs effective {@code VIEW_DEFINITION}, effective {@code CREATE_DEFINITION}, and
   * read access to each requested domain ({@code USE_DOMAIN}) and referenced broader or related
   * term. Returned domains and relations follow the same visibility rules as reads.
   *
   * @param spec The content of the term.
   * @return The created term.
   * @throws TermAlreadyExistsException If a term with the same name or the same source reference
   *     already exists.
   * @throws NoSuchTermException If a broader or related term does not exist or is not visible to
   *     the caller.
   * @throws NotFoundException If a requested domain does not exist.
   * @throws ForbiddenException If the caller lacks the required creation or domain privilege.
   * @throws IllegalArgumentException If a synonym is the name of another term, or a relation is not
   *     allowed.
   */
  GlossaryTerm createTerm(TermSpec spec)
      throws TermAlreadyExistsException, NoSuchTermException, NotFoundException, ForbiddenException,
          IllegalArgumentException;

  /**
   * Alters a term in Step 1. All changes take effect immediately and together: if one fails, none
   * is applied. The revision of the term increases by one. Use {@link
   * TermChange#setStatus(GlossaryTerm.Status)} to change the status without an approval decision.
   *
   * <p>The caller needs effective {@code VIEW_DEFINITION}, plus effective {@code
   * MANAGE_DEFINITIONS} or current ownership with effective {@code CREATE_DEFINITION}. Read access
   * to referenced terms and domains is also required. Returned domains and relations follow the
   * same visibility rules as reads.
   *
   * @param name The name of the term.
   * @param changes The changes to apply.
   * @return The altered term.
   * @throws NoSuchTermException If the term, or a broader or related term in the changes, does not
   *     exist or is not visible to the caller.
   * @throws TermAlreadyExistsException If the term is renamed to the name of another term, or its
   *     source reference is set to the one of another term.
   * @throws NotFoundException If a domain to add does not exist.
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
   * Drops a term, with its synonyms, relations, domain links and bindings. An {@link
   * GlossaryTerm.Status#APPROVED} term cannot be dropped; deprecate it first.
   *
   * <p>The checks run in this order:
   *
   * <ol>
   *   <li>If the term does not exist, or the caller cannot see it, nothing is dropped and the
   *       result is false. Both cases give the same result, so a hidden term stays hidden.
   *   <li>If the caller can see the term but may not drop it, for example a {@link
   *       GlossaryTerm.Status#DEPRECATED} term dropped by its owner without the manage privilege, a
   *       {@link ForbiddenException} is thrown.
   *   <li>If the caller may drop terms but the term is {@link GlossaryTerm.Status#APPROVED}, an
   *       {@link IllegalArgumentException} is thrown.
   * </ol>
   *
   * @param name The name of the term.
   * @return True if the term was dropped, false if it does not exist or is not visible to the
   *     caller.
   * @throws ForbiddenException If the caller can see the term but may not drop it.
   * @throws IllegalArgumentException If the term is {@link GlossaryTerm.Status#APPROVED}.
   */
  boolean dropTerm(String name) throws ForbiddenException, IllegalArgumentException;

  /**
   * Looks up business words, such as the words of a question to an agent. Each word is looked up on
   * its own, and its normalized key is compared with the names and synonyms of the terms. Only
   * exact matches count, so {@code customers} does not find {@code Customer}. Only {@link
   * GlossaryTerm.Status#APPROVED} terms are returned, with {@link GlossaryTerm#matchedBy()} and
   * {@link GlossaryTerm#bindings()} set. The bindings only hold the objects and metrics that the
   * caller can access, at most 20 per term; see {@link GlossaryTerm#bindings()}. A term is returned
   * once, even when it matches several words.
   *
   * <p>The approved-only restriction applies to roots and all nested relation names, even for an
   * owner or a caller with management access who can read drafts through ordinary operations.
   * Hidden domains are omitted. Term status and returned content reflect one consistent read
   * snapshot. No entry means no authorized approved exact match, not that the word has no business
   * meaning. A binding identifies a candidate asset or metric and grants no data access.
   *
   * @param words The words to look up, at most 20.
   * @param domain The name of a domain to limit the lookup to its terms and those of its
   *     subdomains, or null to look up all terms.
   * @return The matched terms. A word that matches no term has no entry.
   * @throws NotFoundException If the domain does not exist or is not visible to the caller.
   * @throws IllegalArgumentException If no word is given, a word is blank, or there are more words
   *     than 20 words.
   */
  GlossaryTerm[] matchTerms(String[] words, @Nullable String domain)
      throws NotFoundException, IllegalArgumentException;
}
