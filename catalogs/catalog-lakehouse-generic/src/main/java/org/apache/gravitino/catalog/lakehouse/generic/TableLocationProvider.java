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
 * provisioning, and {@link #close()} is called when the catalog is closed. Neither {@link
 * #provisionTableLocation(TableLocationContext)} nor {@link
 * #unprovisionTableLocation(TableLocationContext)} is supported after {@link #close()}.
 * Implementations must be thread-safe, because both of them are called concurrently by table
 * requests.
 *
 * <p>Implementations must satisfy two constraints imposed by the {@link java.util.ServiceLoader}
 * based discovery:
 *
 * <ul>
 *   <li>They must have a public no-argument constructor that is cheap and does not throw. Every
 *       provider registered on the classpath is instantiated before the one matching the catalog
 *       property is selected, so a heavy or failing constructor breaks the initialization of every
 *       catalog, including those using the built-in provider. Connection pools, remote clients and
 *       any other expensive setup belong in {@link #initialize(Map)}.
 *   <li>{@link #name()} must be unique across the classpath, and must not be {@value
 *       DefaultTableLocationProvider#NAME}, which is reserved by {@link
 *       DefaultTableLocationProvider}. Two providers sharing a name make every catalog selecting
 *       that name fail to initialize.
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
   * @param catalogProperties the properties of the catalog owning this provider
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
   * <p>A user-supplied {@code location} is visible in {@link
   * TableLocationContext#tableProperties()} and the provider is free to honour or ignore it; the
   * value returned here always wins.
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
   * <p>It is called at most once per dropped table, but a server crash between the removal and this
   * call means it may not be called at all, so an implementation reclaiming real storage needs its
   * own reconciliation to catch those. It must also tolerate being called for a location that is
   * already released. Like {@link #provisionTableLocation(TableLocationContext)}, it is called
   * concurrently and must be thread-safe.
   *
   * @param context the table that was dropped and the context needed to release its location
   */
  void unprovisionTableLocation(TableLocationContext context);

  /**
   * Releases the resources held by this provider. The default implementation does nothing.
   *
   * @throws IOException if closing the underlying resources fails
   */
  @Override
  default void close() throws IOException {}
}
