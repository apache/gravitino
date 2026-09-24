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
package org.apache.gravitino.connector;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.TableCatalog;

/**
 * A server-internal, connector-side capability that maps a normalized table identifier to the
 * identifier under which the table is physically stored by the underlying source, for backends
 * whose name normalization is not reversible.
 *
 * <p>Most catalogs store a table under exactly the name Gravitino normalized it to, so they do not
 * implement this. A catalog whose {@link org.apache.gravitino.connector.capability.Capability}
 * folds an unquoted name to a fixed case while the source also keeps case-sensitive names created
 * with a different case (so a name returned by {@link TableCatalog#listTables} may not equal the
 * normalized name) may implement this so that a name returned by list round-trips through
 * load/alter/drop.
 *
 * <p>This is a {@link CatalogOperations} mixin, not part of the user-facing {@link TableCatalog}
 * API: it is only consulted by the server on the load/alter/drop path and is never exposed to
 * clients.
 *
 * <p><b>The identifier passed in is the normalized one.</b> That is the correct key even though the
 * caller's original spelling is not passed: a case-folding capability already encodes the caller's
 * case intent in the normalized name. For example a backend that folds unquoted names but preserves
 * quoted ones normalizes an unquoted {@code foo} to the folded form (say {@code FOO}) and a quoted
 * {@code "foo"} to {@code foo}; the resolver therefore receives the exact stored-name intent and
 * does not need the raw request string.
 *
 * <p><b>Resolution contract.</b> Given the normalized identifier, an implementation must:
 *
 * <ul>
 *   <li>return {@code normalizedIdent} unchanged if a table with that exact stored name exists (the
 *       common case; no differently-cased object is consulted);
 *   <li>otherwise, if exactly one stored name matches {@code normalizedIdent} case-insensitively,
 *       return that stored name;
 *   <li>otherwise — no match, or several case-insensitive matches with no exact match — return
 *       {@code normalizedIdent} unchanged, so the operation proceeds against the normalized name
 *       and the usual not-found behavior surfaces. An ambiguous name must never be resolved to an
 *       arbitrary object.
 * </ul>
 *
 * <p>Implementations must be side-effect free, must not open connections beyond what the catalog
 * already holds, and must not throw when the table is absent (they return {@code normalizedIdent}
 * so callers such as {@code tableExists} and {@code dropTable} keep their boolean not-found
 * semantics). The server calls this inside the same tree lock it takes for the load/alter/drop
 * operation, so resolution and the operation act atomically on the resolved name.
 */
@Evolving
public interface SupportsTableNameResolution {

  /**
   * Resolves a normalized table identifier to the identifier under which the table is physically
   * stored.
   *
   * @param normalizedIdent The table identifier after Gravitino's case normalization.
   * @return The identifier under which the table is physically stored, or {@code normalizedIdent}
   *     unchanged when no unambiguous mapping applies.
   */
  NameIdentifier resolveTableName(NameIdentifier normalizedIdent);
}
