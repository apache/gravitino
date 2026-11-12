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
 * <p><b>The catalog reports; the provider decides.</b> The catalog's part is to hand over an
 * accurate account of what happened -- what the creation request asked for, where the table ended
 * up, whether the data under a location is gone -- and the provider's part is to decide what that
 * account means for the storage it manages. The catalog does not model what an implementation
 * keeps, and an implementation is never asked to reconstruct what the catalog or the table formats
 * did.
 *
 * <p>There is one exception, and it is written here as an exception rather than dressed up as the
 * rule: an external table that is <b>dropped</b> does not reach {@link
 * #unprovisionTableLocation(TableLocationContext)} at all. That is the one path where a provider
 * acting on an accurate report could still destroy data irrecoverably, because the table formats
 * leave an external dataset in place on a drop. A leaked location can be reconciled and deleted
 * data cannot, so the catalog withholds the call rather than making it.
 *
 * <p><b>Responsibilities.</b> A provider owns the right to use a path; the table format owns the
 * content at the path. The dividing line is not physical against logical -- a provider may well
 * create real infrastructure -- but what a thing was created for: anything brought into being so
 * that the path can be used belongs to the provider, and anything written into the path belongs to
 * the format. Concretely a provider owns its reservation, its registry entry, whatever quota or
 * grant it books, the name itself, and any container it created to make the path usable; a table
 * format owns the dataset, including deleting it; and the catalog owns neither, storing the
 * location string verbatim and sequencing the two calls below.
 *
 * <p><b>Scope.</b> What this interface offers is allocation hooks plus best-effort release
 * notification, with recovery owned by the provider and the deployment. It is deliberately not a
 * distributed transaction and not a recovery system. The limitations below are real, and an
 * implementation that manages storage has to reconcile against the catalog independently of these
 * two calls.
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
 * <p><b>Known limitations.</b> Six of them, and they all point the same way: a provider that
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
 *   <li>A location that is provisioned but then not used is not handed back either. A creation mode
 *       such as Lance's {@code EXIST_OK} returns the table that already exists, at the location it
 *       already has, and the location provisioned for that call goes nowhere. Detecting that from
 *       here would mean comparing two location strings and concluding from the comparison what an
 *       implementation did internally, which is exactly the inference this interface leaves to the
 *       provider; and a retried creation is the ordinary way to reach it, so an implementation that
 *       allocates has to reconcile these along with the failures above.
 *   <li>A table format that drops a table through its own internals, rather than through the
 *       catalog, does not trigger {@link #unprovisionTableLocation(TableLocationContext)}. Lance's
 *       {@code OVERWRITE} creation mode does this: it drops the existing table and creates a new
 *       one, so the old location is never handed back. A format-internal drop is not visible to the
 *       catalog, so this cannot be closed from here.
 *   <li>A provider deriving a deterministic path from a table's identity hands out the same path
 *       again when a table of the same name is created after the old one is gone, and the built-in
 *       provider is one such provider. If anything survived under that path -- a drop whose data
 *       deletion failed, or a format-internal drop as above -- the next creation of that name fails
 *       inside the table format rather than here, and keeps failing, because every retry derives
 *       the same path. Clearing what was left behind is outside what this interface can see or do.
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
   * Returns the name this provider is selected by, matched against the {@code
   * table-location-provider} catalog property.
   *
   * <p>It must be unique across the classpath and must not be {@value
   * DefaultTableLocationProvider#NAME}. It is read once per lookup, on every registered provider,
   * so it must be cheap and must not throw.
   *
   * @return the name of this provider, never null or blank
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
   * <p><b>What the returned location promises.</b> Exactly one thing: that the location is usable,
   * meaning a table format may create its dataset there and will not be refused for any reason
   * under the provider's control. Whether an implementation had to reserve, register or create
   * anything to make that true is its own business, neither required nor forbidden here. It does
   * <em>not</em> promise that a directory or prefix exists, that a dataset exists, or that the
   * location is empty -- the last one deliberately, because in this catalog a caller usually
   * supplies a location precisely when the data is already there.
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
   * Unprovisions the location of a table that is gone, so that a provider allocating through an
   * external service can hand back what it issued instead of leaking it. The location in question
   * is the {@code location} entry of {@link TableLocationContext#tableProperties()}.
   *
   * <p>There is deliberately no default implementation. A provider allocating from an external
   * system has to state what happens when the table goes away, and a provider deriving the path
   * from configuration writes an empty body and says so; an inherited empty body would let the
   * second answer be given by accident.
   *
   * <p><b>What it releases.</b> What the implementation issued: the reservation, the registry
   * entry, whatever quota or grant it booked, and the name itself so that it can be handed out
   * again. An implementation may also remove a container it created itself -- a prefix or a bucket
   * it brought into being so the path could be used -- but only after establishing that the
   * container holds nothing except what the implementation itself put there, and it must leave the
   * container in place otherwise.
   *
   * <p><b>What it must not touch.</b> The content at the location. The table format owns the
   * lifecycle of the data and has already deleted it on every path that reaches this method;
   * deleting anything further is at best redundant and at worst destroys data this catalog has
   * promised not to touch. Nor does the location string establish ownership on its own: the catalog
   * supplies one fact, that nothing anyone needs is under that location any more, and whether the
   * implementation holds anything there is a fact only the implementation can establish, from its
   * own records.
   *
   * <p>It is called <em>after</em> the table metadata, and the underlying data of a managed table,
   * have been removed, so throwing does not roll the drop back: the table is gone either way. The
   * failure is logged at WARN and the drop still reports success. Dropping a schema with cascade
   * unprovisions the location of every table it contains, one by one.
   *
   * <p>It is <em>not</em> called when an external table is <b>dropped</b>, which is the exception
   * named in this interface's description. The catalog does not own their data -- the table formats
   * leave the dataset in place on drop -- so asking a provider to hand the location back would
   * invite it to delete exactly the data the catalog just promised not to touch. A leak is
   * recoverable and a deletion is not, so the callback is skipped.
   *
   * <p>It <em>is</em> called when an external table is <b>purged</b>. Purge and drop do not remove
   * the same things: {@code LanceTableOperations#purgeTable} deletes the external dataset that its
   * {@code dropTable} leaves alone, so by the time this runs the data is gone and the reason to
   * skip has gone with it.
   *
   * <p>Which means this callback is reached in exactly three situations: a dropped managed table, a
   * purged managed table, and a purged external one. There is deliberately no flag distinguishing
   * them, because the invariant they share is the one that matters, and it is about the location
   * rather than the table: <b>nothing anyone needs is under the location named in the context</b>.
   * In all three the table format has already deleted the data under it.
   *
   * <p>Even so, {@code external} is only an approximation of the rule this callback actually wants,
   * which is "hand back only what was handed out". An external table created without a location
   * <em>does</em> get one provisioned -- both table formats check the location after the catalog
   * has filled it in -- and skipping its drop leaks that one. Distinguishing exactly would need the
   * catalog to record, per table, whether it provisioned the location, which it does not do today.
   * That is why an implementation reclaiming real storage needs its own reconciliation, and why
   * this method must tolerate a location it does not recognize.
   *
   * <p>It is called once per dropped table, but it is not guaranteed to be called at all. A crash
   * between the removal and this call skips it, and so does a failure raised inside the drop after
   * the metadata is already gone, which takes no crash and leaves the server running normally. An
   * implementation reclaiming real storage needs its own reconciliation to catch both. It must also
   * tolerate being called for a location that is already released. Like {@link
   * #provisionTableLocation(TableLocationContext)}, it is called concurrently and must be
   * thread-safe.
   *
   * @param context the table that is gone and the context needed to release its location
   */
  void unprovisionTableLocation(TableLocationContext context);
}
