/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.connector;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Table;

/**
 * An optional mixin for {@link org.apache.gravitino.rel.TableCatalog} implementations that can
 * answer a table load from Gravitino's own metadata alone, without contacting the storage system
 * that holds the table.
 *
 * <p>The two loads form a pair with complementary guarantees, and a caller picks the one whose
 * guarantee it needs:
 *
 * <ul>
 *   <li>{@link org.apache.gravitino.rel.TableCatalog#loadTable} is the <b>full</b> load. It
 *       verifies the stored metadata against the underlying table on every call and so returns a
 *       schema that is fresh. When it cannot reach the underlying table it fails with {@link
 *       org.apache.gravitino.exceptions.ConnectionFailedException} rather than returning what it
 *       has, because returning unverified metadata from a load that promises freshness would make
 *       the promise meaningless.
 *   <li>{@link #loadTableLight} is the <b>light</b> load. It never contacts the storage system, so
 *       it stays available when the storage system does not, and it never writes to the entity
 *       store. In exchange its schema may be stale, and callers that need a schema must not use it.
 * </ul>
 *
 * <p>A caller that can tolerate a stale schema should prefer the light load: besides being cheaper,
 * it is the path that survives a storage outage. A caller that needs a fresh schema should use the
 * full load and handle its failure, falling back to the light load only if it can then say that
 * what it returns is unverified.
 *
 * <p><b>Why this interface lives in {@code org.apache.gravitino.connector} and must stay here:</b>
 * it is implemented by a connector inside its own {@link
 * org.apache.gravitino.utils.IsolatedClassLoader} and tested with {@code instanceof} by the server.
 * That only works when both sides resolve the same {@link Class}. {@code IsolatedClassLoader}
 * decides this by package: names outside its {@code isCatalogClass} list are delegated to the
 * server's ClassLoader, which serves them from {@code gravitino-core.jar} on the main classpath.
 * Moving this interface into a module that ships only inside connector and service package
 * directories — {@code lance-common}, for one — would give each ClassLoader its own copy, and the
 * {@code instanceof} below would silently be false everywhere except in tests, which run all
 * catalogs in a single ClassLoader.
 */
@Evolving
public interface SupportsLightTableLoad {

  /**
   * Loads a table from Gravitino's stored metadata without contacting the underlying storage
   * system.
   *
   * <p>Implementations must not open, read or otherwise reach the table's storage, must not check
   * the table's version against it, and must not write to the entity store. The returned schema is
   * whatever Gravitino last stored, which may lag the underlying table.
   *
   * @param ident the identifier of the table to load.
   * @return the table as Gravitino currently has it stored.
   * @throws NoSuchTableException if the table does not exist in Gravitino's metadata.
   */
  Table loadTableLight(NameIdentifier ident) throws NoSuchTableException;
}
