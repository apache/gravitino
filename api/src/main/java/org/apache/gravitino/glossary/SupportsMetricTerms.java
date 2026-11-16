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

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchTermException;

/**
 * Interface for listing and binding the glossary terms of the metrics of a semantic model. A metric
 * uses the bound terms ({@link TermBinding.Kind#USED_BY}). A semantic model itself has no terms;
 * only its metrics do.
 *
 * <p>API inputs identify metrics by their exact name within this semantic model. The server
 * resolves them to existing live metric IDs for storage; model/name is not the physical binding
 * key. Content edits and semantic-model renames preserve metric IDs and bindings. A metric rename
 * currently retires the old identity and creates a new one, so it loses its bindings. Removing and
 * later reintroducing the same name must not revive the retired identity's bindings.
 *
 * <p>Reads authorize semantic-model read access before checking metrics or listing their terms.
 * Glossary view or management privileges grant no semantic-model access. Returned terms, including
 * their domains and nested relations, follow {@link GlossaryTerm}'s visibility rules.
 *
 * <p>All term read and write checks also require effective {@code VIEW_DEFINITION}, using the
 * grant-implication and explicit-deny rules in {@link GlossaryOperations}. Ownership, create or
 * manage access cannot bypass an explicit view denial.
 */
@Evolving
public interface SupportsMetricTerms {

  /**
   * Lists the names of the terms used by a metric that are visible to the caller.
   *
   * @param metric The name of the metric.
   * @return The names of the terms.
   * @throws ForbiddenException If the caller cannot read the semantic model.
   * @throws NoSuchMetadataObjectException If the semantic model has no metric with this name.
   */
  String[] listMetricTerms(String metric) throws NoSuchMetadataObjectException;

  /**
   * Lists the terms used by a metric that are visible to the caller, with details.
   *
   * @param metric The name of the metric.
   * @return The terms.
   * @throws ForbiddenException If the caller cannot read the semantic model.
   * @throws NoSuchMetadataObjectException If the semantic model has no metric with this name.
   */
  GlossaryTerm[] listMetricTermsInfo(String metric) throws NoSuchMetadataObjectException;

  /**
   * Binds terms to a metric and unbinds others, in one change. Binding a bound term, or unbinding a
   * term that is not bound, has no effect. If one term fails, nothing changes.
   *
   * <p>Each term is checked on its own. The caller needs either the admin privilege on the semantic
   * model and read access to the term, or the edit privilege on the term and read access to the
   * semantic model. Otherwise the call fails with a {@link ForbiddenException}.
   *
   * <p>In Step 1, edit access means effective {@code MANAGE_DEFINITIONS}, or current ownership
   * (including group membership) with effective {@code CREATE_DEFINITION}. A hidden or missing term
   * fails with {@link NoSuchTermException} before checking its status. The term-editor path permits
   * any status and takes precedence over the object-admin-only path, which requires approved terms.
   * These checks apply to additions and removals. Step 1 applies changes immediately; Step 2
   * binding proposals require review.
   *
   * @param metric The name of the metric.
   * @param termsToAdd The names of the terms to bind.
   * @param termsToRemove The names of the terms to unbind.
   * @return The names of the terms bound to the metric after the change.
   * @throws NoSuchMetadataObjectException If the semantic model has no metric with this name.
   * @throws NoSuchTermException If a term does not exist or is not visible to the caller.
   * @throws IllegalArgumentException If an admin of the semantic model, who is not an editor of the
   *     term, binds a term that is not {@link GlossaryTerm.Status#APPROVED}.
   * @throws ForbiddenException If the caller may not bind or unbind one of the terms.
   */
  String[] associateMetricTerms(String metric, String[] termsToAdd, String[] termsToRemove)
      throws NoSuchMetadataObjectException, NoSuchTermException, IllegalArgumentException,
          ForbiddenException;
}
