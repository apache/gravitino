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

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link TableLocationProvider} registered only in the test classpath, used to verify that a
 * custom provider is discovered, initialized and closed by the generic catalog.
 */
public class FakeTableLocationProvider implements TableLocationProvider {

  /** The name under which this provider is registered. */
  public static final String NAME = "testing";

  /** The prefix of every location allocated by this provider. */
  public static final String LOCATION_PREFIX = "testing://bucket/";

  // The provider instance used by a catalog is created by the ServiceLoader and not reachable from
  // the test, so the provision and unprovision calls are recorded statically.
  private static final List<TableLocationContext> PROVISIONED =
      Collections.synchronizedList(Lists.newArrayList());

  private static final List<TableLocationContext> UNPROVISIONED =
      Collections.synchronizedList(Lists.newArrayList());

  private static final List<TableLocationContext> RELEASED =
      Collections.synchronizedList(Lists.newArrayList());

  private static final AtomicInteger CLOSED = new AtomicInteger();

  // Selection has to construct every registered provider to ask it its name, so counting
  // constructions is how a test observes whether a lookup scanned or answered from the index.
  private static final AtomicInteger CONSTRUCTED = new AtomicInteger();

  private static volatile boolean failOnUnprovision;

  private static volatile boolean failOnInitialize;

  private static volatile boolean overrideLocation;

  private static volatile String locationOverride;

  private Map<String, String> catalogProperties;

  private boolean closed;

  /** Public and no-argument, as the SPI requires; it acquires nothing. */
  public FakeTableLocationProvider() {
    CONSTRUCTED.incrementAndGet();
  }

  /**
   * Returns the contexts this provider was asked to provision, in the order the calls came in, so
   * that a test can assert the provider was or was not consulted for a given table.
   *
   * @return the recorded provision contexts
   */
  public static List<TableLocationContext> provisioned() {
    return Lists.newArrayList(PROVISIONED);
  }

  /**
   * Returns the contexts this provider was asked to unprovision, in the order the calls came in.
   *
   * @return the recorded unprovision contexts
   */
  public static List<TableLocationContext> unprovisioned() {
    return Lists.newArrayList(UNPROVISIONED);
  }

  /**
   * Returns the contexts this provider was asked to release as unused, in the order the calls came
   * in. Kept apart from {@link #unprovisioned()} so that a test can tell the drop callback from the
   * creation-path one, which a provider reclaiming by table identity has to tell apart too.
   *
   * @return the recorded release contexts
   */
  public static List<TableLocationContext> released() {
    return Lists.newArrayList(RELEASED);
  }

  /**
   * Makes every subsequent unprovision call fail, to verify how the catalog reports a provider that
   * cannot reclaim a location.
   *
   * @param fail whether unprovisioning should throw
   */
  public static void failOnUnprovision(boolean fail) {
    failOnUnprovision = fail;
  }

  /**
   * Makes every subsequent initialization fail, to verify that a provider failing halfway through
   * {@link #initialize(Map)} is still closed.
   *
   * @param fail whether initialization should throw
   */
  public static void failOnInitialize(boolean fail) {
    failOnInitialize = fail;
  }

  /**
   * Returns how many instances of this provider have been closed since the last {@link #reset()},
   * so that a test can observe an instance it never gets a reference to.
   *
   * @return the number of recorded close calls
   */
  public static int closedCount() {
    return CLOSED.get();
  }

  /**
   * Returns how many instances have been constructed since the last {@link #reset()}, so that a
   * test can tell a lookup that scanned the classpath from one answered out of the cached index.
   *
   * @return the number of constructions
   */
  public static int constructedCount() {
    return CONSTRUCTED.get();
  }

  /**
   * Makes every subsequent provision call return the given location instead of the composed one, so
   * that a test can drive the catalog with a location a real provider should never return.
   *
   * @param location the location to return, null included
   */
  public static void provisionLocation(String location) {
    overrideLocation = true;
    locationOverride = location;
  }

  /** Clears the recorded calls and stops initialization and unprovisioning from failing. */
  public static void reset() {
    overrideLocation = false;
    locationOverride = null;
    PROVISIONED.clear();
    UNPROVISIONED.clear();
    RELEASED.clear();
    CLOSED.set(0);
    CONSTRUCTED.set(0);
    failOnUnprovision = false;
    failOnInitialize = false;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.catalogProperties = catalogProperties;
    if (failOnInitialize) {
      throw new IllegalStateException("The path allocation service is unreachable");
    }
  }

  @Override
  public String provisionTableLocation(TableLocationContext context) {
    PROVISIONED.add(context);
    return overrideLocation
        ? locationOverride
        : LOCATION_PREFIX + context.tableIdentifier().name() + "/";
  }

  @Override
  public void unprovisionTableLocation(TableLocationContext context) {
    UNPROVISIONED.add(context);
    if (failOnUnprovision) {
      throw new IllegalStateException("The path allocation service is unavailable");
    }
  }

  @Override
  public void releaseUnusedLocation(TableLocationContext context) {
    RELEASED.add(context);
  }

  @Override
  public void close() {
    this.closed = true;
    CLOSED.incrementAndGet();
  }

  /**
   * Returns the catalog properties this provider was initialized with, or null if {@link
   * #initialize(Map)} was never called.
   *
   * @return the catalog properties seen at initialization time
   */
  public Map<String, String> catalogProperties() {
    return catalogProperties;
  }

  /**
   * Returns whether {@link #close()} has been called on this instance.
   *
   * @return true if this provider was closed
   */
  public boolean isClosed() {
    return closed;
  }
}
