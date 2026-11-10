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
package org.apache.gravitino.catalog.lakehouse.generic;

import javax.annotation.Nullable;

/**
 * An optional capability of a {@link TableLocationProvider}: allocating the location of a schema
 * when it is created, and releasing it when the schema is dropped.
 *
 * <p>A provider opts in by implementing this interface alongside {@link TableLocationProvider}.
 * Services that allocate storage usually do so hierarchically -- a schema is registered first, with
 * the owner and the storage domain it belongs to, and the paths of its tables are derived from it
 * -- and the table callbacks alone offer no point at which to register the schema.
 *
 * <p>A provider that does not implement it is never consulted for schemas, and the catalog does no
 * extra work on its behalf: dropping an empty schema still does not read the schema. That is why
 * this is a separate interface rather than default methods on {@link TableLocationProvider}.
 *
 * <p>The division of responsibilities is the one {@link TableLocationProvider} describes. The
 * provider owns what it brought into being so that the schema location can be used -- its
 * registration, its quota, any container it created -- and the table formats own the data written
 * under it. Like the table callbacks, both methods here are called concurrently and must be
 * thread-safe, and the catalog applies no timeout to them.
 *
 * <p><b>Known limitations.</b>
 *
 * <ul>
 *   <li>{@link #provisionSchemaLocation(SchemaLocationContext)} is called before the schema is
 *       stored. A creation that fails afterwards -- a schema of the same name created concurrently,
 *       or a store failure -- leaves a location provisioned for a schema that does not exist, and
 *       there is no compensating unprovision. A provider that answers idempotently may have
 *       returned the allocation of the schema that won the race, and releasing it would take it
 *       from a schema that exists. A schema that already exists is detected before the provider is
 *       consulted, so the cases that remain are the races.
 *   <li>{@code location} is a schema property, and altering it does not tell the provider.
 *   <li>{@link #unprovisionSchemaLocation(SchemaLocationContext)} is called after the schema is
 *       removed, and not at all if the server stops in between, so a provider reclaiming real
 *       storage needs its own reconciliation against the catalog.
 * </ul>
 */
public interface SupportsSchemaLocations {

  /**
   * Provisions the location of the schema that is being created.
   *
   * <p>The context carries the properties of the creation request, including a caller-supplied
   * {@code location}, if any. As with tables, what happens to a supplied location is the provider's
   * decision: return null to keep it, return something else to place the schema elsewhere, or throw
   * to refuse the creation.
   *
   * <p>A non-null result is stored verbatim as the schema's {@code location} property. It must not
   * be blank; the catalog rejects the creation with an {@link IllegalArgumentException} otherwise.
   * Because the built-in provider derives the location of a table from its schema's {@code
   * location}, recording it also keeps the schema consistent with the tables placed under it.
   *
   * <p>If this method throws, the schema is not created.
   *
   * @param context the schema being created, with the properties of the creation request
   * @return the location to store on the schema, or null to keep the properties of the request
   *     unchanged
   */
  @Nullable
  String provisionSchemaLocation(SchemaLocationContext context);

  /**
   * Releases what this provider allocated for a schema that has been dropped.
   *
   * <p>It is called only after the schema has been removed. With cascade, the tables of the schema
   * are dropped first, through the same path as a single table drop, so their {@link
   * TableLocationProvider#unprovisionTableLocation(TableLocationContext)} callbacks have already
   * run. The context carries the stored properties of the schema, read right before it was removed,
   * whether the drop cascaded, and which of the cascaded tables kept their data.
   *
   * <p><b>The catalog reports; the provider decides.</b> A table that kept its data is an external
   * table that was dropped rather than purged: the table format left its dataset in place and its
   * location was deliberately not unprovisioned. When the context lists any, the schema location
   * may still hold data, and the provider decides whether to release it anyway, keep it, or keep it
   * and report it. There is deliberately no force option on the drop: the decision belongs to the
   * provider, as it does for tables.
   *
   * <p>What the provider must not touch is the same as for tables: the data. It may remove a
   * container it created itself only once that container holds nothing but what the provider put
   * there.
   *
   * <p>Throwing does not roll the drop back, because the schema is already gone. The failure is
   * logged at WARN and the drop still reports success.
   *
   * @param context the schema that was dropped and the outcome of the drop
   */
  void unprovisionSchemaLocation(SchemaLocationContext context);
}
