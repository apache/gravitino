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
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchTermException;

/**
 * Interface for listing and binding the glossary terms of a metadata object. The object implements
 * the bound terms ({@link TermBinding.Kind#IMPLEMENTED_BY}). This interface is mixed with the
 * metadata objects of the types in {@link TermBinding#IMPLEMENTED_BY_TYPES}.
 *
 * <p>Reads first require access to the target metadata object, before enumerating columns or
 * reading bindings. A column requires read access to its containing table. Glossary view or
 * management privileges do not replace target access. Only visible terms are returned, and their
 * domains and nested relations follow {@link GlossaryTerm}'s visibility rules. Step 1 supports
 * bindings to a view itself, but not to view output columns.
 */
@Evolving
public interface SupportsTerms {

  /**
   * Lists the names of the terms bound to the object.
   *
   * @return The names of the terms.
   * @throws ForbiddenException If the caller cannot read the target object.
   */
  String[] listTerms();

  /**
   * Lists the terms bound to the object, with details.
   *
   * @return The terms.
   * @throws ForbiddenException If the caller cannot read the target object.
   */
  GlossaryTerm[] listTermsInfo();

  /**
   * Binds terms to the object and unbinds others, in one change. Binding a bound term, or unbinding
   * a term that is not bound, has no effect. If one term fails, nothing changes.
   *
   * <p>Each term is checked on its own. The caller needs either the admin privilege on the object
   * and read access to the term, or the edit privilege on the term and read access to the object.
   * Otherwise the call fails with a {@link ForbiddenException}.
   *
   * <p>In Step 1, edit access means effective {@code MANAGE_DEFINITIONS}, or current ownership
   * (including group membership) with effective {@code CREATE_DEFINITION}. A hidden or missing term
   * fails with {@link NoSuchTermException} before checking its status. The term-editor path permits
   * any status and takes precedence even when the caller is also an object admin. Only the
   * object-admin-only path requires approved terms. These checks apply to both additions and
   * removals. Step 1 applies changes immediately; Step 2 binding proposals require review.
   *
   * @param termsToAdd The names of the terms to bind.
   * @param termsToRemove The names of the terms to unbind.
   * @return The names of the terms bound to the object after the change.
   * @throws NoSuchTermException If a term does not exist or is not visible to the caller.
   * @throws IllegalArgumentException If an admin of the object, who is not an editor of the term,
   *     binds a term that is not {@link GlossaryTerm.Status#APPROVED}.
   * @throws ForbiddenException If the caller may not bind or unbind one of the terms.
   */
  String[] associateTerms(String[] termsToAdd, String[] termsToRemove)
      throws NoSuchTermException, IllegalArgumentException, ForbiddenException;

  /**
   * Lists the terms bound to each column of a table, in one call. Reading the table must be
   * authorized before enumerating columns. View-column terms are outside Step 1, even when the view
   * supports its own term bindings.
   *
   * @return The terms of each column that has visible terms, by column name.
   * @throws ForbiddenException If the caller cannot read the table.
   * @throws UnsupportedOperationException If table-column term operations are not supported,
   *     including when the object is a view.
   */
  default Map<String, GlossaryTerm[]> listColumnTerms() {
    throw new UnsupportedOperationException("Table-column term operations are not supported.");
  }
}
