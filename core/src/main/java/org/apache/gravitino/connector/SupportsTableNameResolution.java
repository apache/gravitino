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
 * A server-internal, connector-side capability that maps a table identifier to the identifier under
 * which the table is physically stored by the underlying source, for backends whose name
 * normalization is not reversible.
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
 * clients. The server resolves the name before the operation runs, so the resolved identifier
 * drives the downstream authorization hooks, the underlying catalog call and the Gravitino entity
 * store key consistently.
 *
 * <p><b>Resolution contract.</b> Implementations receive both the identifier the caller requested
 * and the identifier after Gravitino's case normalization, and must:
 *
 * <ul>
 *   <li>prefer an object whose stored name equals {@code requestedIdent}'s name exactly, so a
 *       case-sensitive name the caller supplied verbatim is honored even when a differently-cased
 *       sibling exists;
 *   <li>otherwise use an object whose stored name equals {@code normalizedIdent}'s name exactly;
 *   <li>otherwise, if exactly one stored name matches {@code normalizedIdent} case-insensitively,
 *       use that stored name;
 *   <li>otherwise — no match, or several case-insensitive matches with no exact match — return
 *       {@code normalizedIdent} unchanged, so the operation proceeds against the normalized name
 *       and the usual not-found behavior surfaces. An ambiguous name must never be resolved to an
 *       arbitrary object.
 * </ul>
 *
 * <p>Implementations must be side-effect free, must not open connections beyond what the catalog
 * already holds, and must not throw when the table is absent (they return {@code normalizedIdent}
 * so callers such as {@code tableExists} and {@code dropTable} keep their boolean not-found
 * semantics). Resolution is best-effort: it runs before the server acquires its per-table lock, so
 * a concurrent rename/recreate simply surfaces as the normal {@code NoSuchTableException} from the
 * subsequent locked operation, never as an action on a different object.
 */
@Evolving
public interface SupportsTableNameResolution {

  /**
   * Resolves a table identifier to the identifier under which the table is physically stored.
   *
   * @param requestedIdent The identifier as requested by the caller, before case normalization.
   * @param normalizedIdent The identifier after Gravitino's case normalization.
   * @return The identifier under which the table is physically stored, or {@code normalizedIdent}
   *     unchanged when no unambiguous mapping applies.
   */
  NameIdentifier resolveTableName(NameIdentifier requestedIdent, NameIdentifier normalizedIdent);
}
