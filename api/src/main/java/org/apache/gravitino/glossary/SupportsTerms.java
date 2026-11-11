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
import org.apache.gravitino.exceptions.NoSuchTermException;

/**
 * Interface for listing and binding the glossary terms of a metadata object. The object implements
 * the bound terms ({@link TermBinding.Kind#IMPLEMENTED_BY}). This interface is mixed with the
 * metadata objects of the types in {@link TermBinding#IMPLEMENTED_BY_TYPES}.
 *
 * <p>Only the terms visible to the caller are returned.
 */
@Evolving
public interface SupportsTerms {

  /**
   * Lists the names of the terms bound to the object.
   *
   * @return The names of the terms.
   */
  String[] listTerms();

  /**
   * Lists the terms bound to the object, with details.
   *
   * @return The terms.
   */
  GlossaryTerm[] listTermsInfo();

  /**
   * Binds terms to the object and unbinds others, in one change. Binding a bound term, or unbinding
   * a term that is not bound, has no effect.
   *
   * @param termsToAdd The names of the terms to bind.
   * @param termsToRemove The names of the terms to unbind.
   * @return The names of the terms bound to the object after the change.
   * @throws NoSuchTermException If a term does not exist or is not visible to the caller.
   * @throws IllegalArgumentException If a term cannot be bound by the caller, for example a term
   *     that is not {@link GlossaryTerm.Status#APPROVED} bound by an admin of the object.
   */
  String[] associateTerms(String[] termsToAdd, String[] termsToRemove)
      throws NoSuchTermException, IllegalArgumentException;

  /**
   * Lists the terms bound to each column of a table or a view, in one call.
   *
   * @return The terms of each column that has visible terms, by column name.
   * @throws UnsupportedOperationException If the object is not a table or a view.
   */
  default Map<String, GlossaryTerm[]> listColumnTerms() {
    throw new UnsupportedOperationException("Only tables and views have column terms.");
  }
}
