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

/**
 * A pluggable strategy for deciding where the data of a newly created table lives.
 *
 * <p>Implementations are discovered through Java's {@link java.util.ServiceLoader} and selected by
 * {@link #name()} using the {@code table-location-provider} catalog property. The built-in {@link
 * DefaultTableLocationProvider} derives the location from the table, schema and catalog {@code
 * location} properties; deployments that allocate storage through an external service can register
 * their own implementation instead.
 *
 * <p>The interface is deliberately two operations wide: hand out a location for a table being
 * created, and hand one back for a table that is gone. Every decision that can be made from what
 * the catalog already knows is made by the catalog, so that an implementation has as little to get
 * right as possible. In particular, whether an external table's data survives a drop is decided
 * here and not there -- see {@link #unprovisionTableLocation(TableLocationContext)}.
 *
 * <p><b>There is no lifecycle.</b> An instance is created per catalog and is never initialized or
 * closed by the catalog, so an implementation needing configuration of its own -- a service
 * endpoint, a credential -- has to obtain it without help from here, and anything it acquires is
 * held for the lifetime of the instance with no callback to release it. An implementation holding a
 * remote client should therefore acquire it lazily and make it safe to abandon, because catalogs
 * are evicted from the server's catalog cache when idle and a discarded provider is not told.
 *
 * <p>Implementations must be thread-safe: {@link #provisionTableLocation(TableLocationContext)} and
 * {@link #unprovisionTableLocation(TableLocationContext)} are both called concurrently by table
 * requests. A failure on the provisioning path fails the table creation; one on the drop path is
 * logged at WARN and nothing else happens.
 *
 * <p>Implementations must satisfy two constraints imposed by the {@link java.util.ServiceLoader}
 * based discovery:
 *
 * <ul>
 *   <li>They must have a public no-argument constructor that is cheap, does not throw and acquires
 *       nothing. Every provider registered on the classpath is instantiated before the one matching
 *       the catalog property is selected, so a heavy constructor slows the initialization of every
 *       catalog, including those using the built-in provider. One that throws is logged and skipped
 *       rather than failing the lookup, which costs that provider the ability to be selected at
 *       all. The instances that were not selected are then discarded, and nothing is closed on
 *       them, so anything a constructor acquires is leaked once per catalog creation.
 *   <li>{@link #name()} must be unique across the classpath, and must not be {@value
 *       DefaultTableLocationProvider#NAME}, which is reserved by {@link
 *       DefaultTableLocationProvider}. If two providers share a name, every catalog selecting that
 *       name fails to initialize. A provider whose constructor or {@link #name()} throws is logged
 *       and skipped rather than failing the lookup, so a provider that is broken at runtime does
 *       not stop catalogs that named a different one from starting. A services file naming a class
 *       that cannot be loaded at all still fails the lookup.
 * </ul>
 *
 * <p><b>Known limitations.</b> Five of them, and they all point the same way: a provider that
 * manages real storage needs its own reconciliation against the catalog and cannot treat the
 * callbacks here as a complete record of what it handed out.
 *
 * <ul>
 *   <li>{@code location} is a mutable table property, so {@code alterTable(setProperty("location",
 *       ...))} repoints a table without this provider being told. The old location is never
 *       unprovisioned and the new one never went through this provider.
 *   <li>{@link #provisionTableLocation(TableLocationContext)} is called before the table is
 *       actually created, so a creation that fails afterwards -- a table that already exists, or a
 *       failure inside the table format itself -- leaves a location provisioned for a table that
 *       does not exist. There is no compensating unprovision, deliberately: a table format that
 *       fails partway through creation may already have written to the location, and calling {@link
 *       #unprovisionTableLocation(TableLocationContext)} would then tell the provider it is free to
 *       reclaim a path that has data on it. Leaking an unused path is the safer of the two
 *       failures, and doing better would need the format to report whether it touched storage
 *       before failing, which this interface cannot express. Every check this catalog can make on
 *       its own is made before the provider is consulted, so the cases that remain are the ones
 *       only the table format can detect.
 *   <li>A creation the table format serves from a table that already exists leaks the location
 *       provisioned for that call. An {@code EXIST_OK}-style creation mode returns the existing
 *       table at the location it already had, and a client retrying a create is the ordinary way to
 *       reach it, so this is not a rare path. The unused location is neither reported nor handed
 *       back, because the table is alive: an implementation that books allocations against {@code
 *       (schema, table)} would read a reclaim keyed by that identity as an instruction to delete a
 *       live table's storage, and there is no callback that would let it tell the two apart. Such
 *       an implementation meets this case on its own, since a retried create arrives as a second
 *       provisioning request for a table it already holds an allocation for.
 *   <li>A table format that drops a table through its own internals, rather than through the
 *       catalog, does not trigger {@link #unprovisionTableLocation(TableLocationContext)}. Lance's
 *       {@code OVERWRITE} creation mode does this: it drops the existing table and creates a new
 *       one, so the old location is never handed back. A format-internal drop is not visible to the
 *       catalog, so this cannot be closed from here.
 *   <li>{@code alterTable(rename(...))} changes a table's identity without telling this provider,
 *       and without moving any data. A provider deriving the path from the table name is left with
 *       a path that no longer matches the name, which is cosmetic. A provider that books
 *       allocations against {@code (schema, table)} loses the table altogether: the drop that
 *       follows arrives under the new name, and the allocation booked under the old one is never
 *       handed back. Such a provider has to reconcile renames out of band, or the deployment has to
 *       forbid renaming tables in this catalog.
 * </ul>
 */
public interface TableLocationProvider {

  /**
   * Returns the name identifying this provider. The value is matched case-insensitively against the
   * {@code table-location-provider} catalog property to select a provider.
   *
   * @return the provider name, never null or blank
   */
  String name();

  /**
   * Provisions the location for the table that is being created.
   *
   * <p>The returned location is stored verbatim in the table's {@code location} property. It must
   * be non-blank; the caller rejects the table creation with an {@link IllegalArgumentException}
   * otherwise. Nothing else is required of it: the shape of the path belongs to the provider,
   * nothing downstream appends to the location, and storing it unchanged is what lets a provider
   * unprovisioning it later match the string it handed out.
   *
   * <p><b>A request that carries its own {@code location} reaches this method too</b>, with the
   * supplied value visible as the {@code location} entry of {@link
   * TableLocationContext#tableProperties()}. The decision is the provider's: return it unchanged to
   * honour it, return something else to place the table elsewhere, or throw to refuse the creation.
   * A provider enforcing a placement policy would have nothing to enforce if the catalog decided
   * this on its behalf, and a caller-supplied path is exactly the case such a policy exists for.
   *
   * <p>An implementation that allocates storage has to handle that case deliberately, because in
   * this catalog a caller usually supplies a location because the data is already there -- an
   * external Delta table, or a Lance registration. Allocating a fresh empty path for one of those
   * and returning it repoints the table at an empty directory and orphans the caller's data, while
   * the creation still reports success. Returning the supplied value unchanged is the safe default
   * and what the built-in provider does as its first branch, which is why a catalog on the built-in
   * provider sees no change.
   *
   * <p>Everything else reaches this method as well, including an external table that carries no
   * location. Whether a table is external can be read from the {@code external} entry of {@link
   * TableLocationContext#tableProperties()}.
   *
   * @param context the table being created and the context needed to derive its location
   * @return the provisioned table location, never null or blank
   * @throws IllegalArgumentException if no location can be derived from the given context
   */
  String provisionTableLocation(TableLocationContext context);

  /**
   * Unprovisions the location of a table that has been dropped, so that a provider allocating
   * storage through an external service can hand it back instead of leaking it. The location to
   * hand back is the {@code location} entry of {@link TableLocationContext#tableProperties()}.
   *
   * <p>There is deliberately no default implementation. A provider allocating from an external
   * system has to state what happens when the table goes away, and a provider deriving the path
   * from configuration writes an empty body and says so; an inherited empty body would let the
   * second answer be given by accident.
   *
   * <p>It is called <em>after</em> the table metadata, and the underlying data of a managed table,
   * have been removed, so throwing does not roll the drop back: the table is gone either way. The
   * failure is logged at WARN and the drop still reports success. Dropping a schema with cascade
   * unprovisions the location of every table it contains, one by one.
   *
   * <p>It is <em>not</em> called when an external table is <b>dropped</b>. The catalog does not own
   * their data -- the table formats leave the dataset in place on drop -- so asking a provider to
   * hand the location back would invite it to delete exactly the data the catalog just promised not
   * to touch. A leak is recoverable and a deletion is not, so the callback is skipped.
   *
   * <p>It <em>is</em> called when an external table is <b>purged</b>. Purge and drop do not remove
   * the same things: {@code LanceTableOperations#purgeTable} deletes the external dataset that its
   * {@code dropTable} leaves alone, so by the time this runs the data is gone and the reason to
   * skip has gone with it.
   *
   * <p>Which means this callback is reached in exactly three situations -- a dropped managed table,
   * a purged managed table, and a purged external one -- and in all three the table is gone and the
   * data under its location has already been deleted by the table format. There is deliberately no
   * flag distinguishing them, because there is nothing left to distinguish: an implementation has
   * the same job in all three, which is to hand the location back. The catalog makes that
   * distinction rather than passing it on, because the catalog is where the knowledge lives about
   * which formats leave data in place.
   *
   * <p>Even so, {@code external} is only an approximation of the rule this callback actually wants,
   * which is "hand back only what was handed out". An external table created without a location
   * <em>does</em> get one provisioned -- both table formats check the location after the catalog
   * has filled it in -- and skipping its drop leaks that one. Distinguishing exactly would need the
   * catalog to record, per table, whether it provisioned the location, which it does not do today.
   * That is why an implementation reclaiming real storage needs its own reconciliation, and why
   * this method must tolerate a location it does not recognize.
   *
   * <p>This method is only ever called for a table that is gone. A location provisioned for a table
   * that then went on to exist somewhere else is never handed back, so an implementation deleting
   * by table identity cannot be told to delete a live table through this callback.
   *
   * <p>It is called once per dropped table, but it is not guaranteed to be called at all. A crash
   * between the removal and this call skips it, and so does a failure raised inside the drop after
   * the metadata is already gone, which takes no crash and leaves the server running normally. An
   * implementation reclaiming real storage needs its own reconciliation to catch both. It must also
   * tolerate being called for a location that is already released. Like {@link
   * #provisionTableLocation(TableLocationContext)}, it is called concurrently and must be
   * thread-safe.
   *
   * @param context the table that was dropped and the context needed to release its location
   */
  void unprovisionTableLocation(TableLocationContext context);
}
