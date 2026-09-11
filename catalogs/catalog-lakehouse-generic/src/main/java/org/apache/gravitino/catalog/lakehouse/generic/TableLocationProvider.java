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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * A pluggable strategy for deciding where the data of a newly created table lives.
 *
 * <p>Implementations are discovered through Java's {@link java.util.ServiceLoader} and selected by
 * {@link #name()} using the {@code table-location-provider} catalog property. The built-in {@link
 * DefaultTableLocationProvider} derives the location from the table, schema and catalog {@code
 * location} properties; deployments that allocate storage through an external service can register
 * their own implementation instead.
 *
 * <p>An instance is created per catalog, {@link #initialize(Map)} is called once before any
 * provisioning, and {@link #close()} is called when the catalog is closed. Implementations must be
 * thread-safe: {@link #provisionTableLocation(TableLocationContext)}, {@link
 * #unprovisionTableLocation(TableLocationContext)} and {@link
 * #releaseUnusedLocation(TableLocationContext)} are all called concurrently by table requests.
 *
 * <p>The catalog does not fence in-flight requests against {@link #close()}, so a request that
 * started before the catalog was closed can reach any of the three callbacks afterwards. An
 * implementation is not asked to keep working across a close; it is asked to fail cleanly rather
 * than corrupt anything, which is what a closed client throwing does on its own. On the drop and
 * release paths the failure is logged at WARN and nothing else happens; on the provisioning path it
 * fails the table creation, which is the right answer for a catalog that is shutting down.
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
 *       all. The instances that were not selected are then discarded without {@link #close()} being
 *       called on them, so anything a constructor acquires is leaked once per catalog creation.
 *       Connection pools, remote clients and any other expensive setup belong in {@link
 *       #initialize(Map)}.
 *   <li>{@link #name()} must be unique across the classpath, and must not be {@value
 *       DefaultTableLocationProvider#NAME}, which is reserved by {@link
 *       DefaultTableLocationProvider}. If two providers share a name, every catalog selecting that
 *       name fails to initialize. A provider that cannot be instantiated at all is logged and
 *       skipped rather than failing the lookup, so one broken jar does not stop every catalog from
 *       starting.
 * </ul>
 *
 * <p><b>Known limitations.</b> Four of them, and they all point the same way: a provider that
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
public interface TableLocationProvider extends Closeable {

  /**
   * Returns the name identifying this provider. The value is matched case-insensitively against the
   * {@code table-location-provider} catalog property to select a provider.
   *
   * @return the provider name, never null or blank
   */
  String name();

  /**
   * Initializes the provider with the properties of the catalog it belongs to. Called exactly once,
   * before any call to {@link #provisionTableLocation(TableLocationContext)}.
   *
   * <p>The default implementation does nothing, because a provider that derives the location purely
   * from {@link TableLocationContext} has nothing to prepare. Providers that hold a remote client,
   * a connection or any state that must outlive a single table creation must override it, and
   * release those resources in {@link #close()}.
   *
   * <p>Throwing from here fails the initialization of the catalog. The instance is closed before
   * the failure propagates, so a provider that acquired part of its resources before giving up
   * still gets to release them in {@link #close()}.
   *
   * @param catalogProperties the properties of the catalog owning this provider, never null and
   *     never modified after this call
   */
  default void initialize(Map<String, String> catalogProperties) {}

  /**
   * Provisions the location for the table that is being created.
   *
   * <p>The returned location is stored verbatim in the table's {@code location} property. It must
   * be non-blank; the caller rejects the table creation with an {@link IllegalArgumentException}
   * otherwise. Nothing else is required of it: the shape of the path belongs to the provider,
   * nothing downstream appends to the location, and storing it unchanged is what lets a provider
   * unprovisioning it later match the string it handed out.
   *
   * <p>A request that carries its own {@code location} never reaches this method; the supplied
   * value is stored, normalized only with a trailing slash. In this catalog a caller supplies a
   * location mostly because the data is already there -- an external Delta table, or a Lance
   * registration -- and allocating a fresh empty path for one of those would orphan the caller's
   * data while still reporting success. The catalog cannot tell those requests apart from a caller
   * merely overriding placement, so it keeps the supplied location in both cases, which is also
   * what it has always done. A deployment that wants allocation to be mandatory has to reject a
   * caller-supplied location before it reaches the catalog; a provider cannot enforce it, because
   * it is not called.
   *
   * <p>Everything else reaches this method, including an external table that carries no location.
   * Whether a table is external can be read from the {@code external} entry of {@link
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
   * <p>It is <em>not</em> called for external tables. The catalog does not own their data -- the
   * table formats leave the dataset in place on drop -- so asking a provider to hand the location
   * back would invite it to delete exactly the data the catalog just promised not to touch. A leak
   * is recoverable and a deletion is not, so the callback is skipped. {@link
   * TableLocationContext#isExternal()} reports the same flag on the paths where it is called.
   *
   * <p>The external flag is an approximation of the rule this callback actually wants, which is
   * "hand back only what was handed out", and two cases stay asymmetric under it. An external table
   * created without a location <em>does</em> get one provisioned -- both table formats check the
   * location after the catalog has filled it in -- and skipping leaks that one. A table that is not
   * external but whose creation carried its own location was never provisioned, yet is still
   * unprovisioned here, so the provider is asked about a path it never issued. Distinguishing them
   * exactly would need the catalog to record, per table, whether it provisioned the location, which
   * it does not do today. Both cases are why an implementation reclaiming real storage needs its
   * own reconciliation, and why this method must tolerate a location it does not recognize.
   *
   * <p>There is no purge flag in the context, because for the tables this catalog manages there is
   * nothing to distinguish: {@code ManagedTableOperations.purgeTable} delegates straight to {@code
   * dropTable}, so the two paths remove exactly the same things. The signal that matters is {@code
   * external}, which is already available.
   *
   * <p>This method is only ever called for a table that is gone. A location provisioned for a table
   * that then went on to exist somewhere else is handed back through {@link
   * #releaseUnusedLocation(TableLocationContext)} instead, which is a separate method precisely so
   * that an implementation deleting by table identity does not delete a live table.
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

  /**
   * Hands back a location that was provisioned for a table creation the table format did not use,
   * so that it is not leaked. The location to release is the {@code location} entry of {@link
   * TableLocationContext#tableProperties()}.
   *
   * <p><b>The table is alive.</b> This is the one callback here that does not mean the table went
   * away: the creation succeeded, the caller is about to be handed the table, and only the location
   * this provider issued for that call went unused, because the format answered with a table living
   * somewhere else. An {@code EXIST_OK}-style creation mode reaching a table that already exists is
   * the ordinary way to get here. An implementation must therefore release <em>the location</em>
   * and must not delete anything keyed by the table identity in the context, because that identity
   * belongs to a table that exists. This is why the callback is not {@link
   * #unprovisionTableLocation(TableLocationContext)}: for a provider deriving paths from
   * configuration the two are the same operation, but for one that books allocations against {@code
   * (schema, table)} they are opposites, and no runtime flag would have made that difference as
   * hard to overlook as two methods do.
   *
   * <p>For the same reason the {@code external} entry of the properties is not a signal to skip
   * here, though it is on the drop path. The location being released was allocated by this provider
   * moments ago, on request, so nobody else's data can be under it.
   *
   * <p>The default implementation does nothing, which leaks the unused location. That is the
   * deliberate default: not releasing is what this catalog did before the callback existed and
   * costs one stray allocation, whereas releasing something the implementation has misidentified
   * costs live data. A provider that allocates real storage should implement it.
   *
   * <p>This is deliberately the opposite call from {@link
   * #unprovisionTableLocation(TableLocationContext)}, which has no default so that no provider can
   * stay silent about drops by accident. The two differ because the cost of silence differs: there,
   * silence leaks a location on every drop, on the ordinary path, forever; here it leaks one
   * location in the uncommon case that a format declined the one it was given. A provider that
   * cannot release by path -- because the service behind it only deletes by table identity --
   * should leave this method alone and reclaim through its own reconciliation, which the default
   * lets it do without writing a body that would be wrong.
   *
   * <p>The catalog decides that a location went unused by comparing the location it handed the
   * format with the one the created table reports, ignoring a trailing slash. A format that
   * rewrites the location it was given -- normalizing a URI scheme, say -- looks from here like a
   * format that declined it, so a provider whose paths may come back rewritten should verify before
   * reclaiming. Throwing is logged at WARN and does not fail the creation, which already succeeded.
   *
   * @param context the table that was created and the unused location provisioned for it
   */
  default void releaseUnusedLocation(TableLocationContext context) {}

  /**
   * Releases the resources held by this provider. The default implementation does nothing.
   *
   * @throws IOException if closing the underlying resources fails
   */
  @Override
  default void close() throws IOException {}
}
