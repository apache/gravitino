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
  // the test, so the unprovision calls are recorded statically.
  private static final List<TableLocationContext> UNPROVISIONED =
      Collections.synchronizedList(Lists.newArrayList());

  private static volatile boolean failOnUnprovision;

  private Map<String, String> catalogProperties;

  private boolean closed;

  /**
   * Returns the contexts this provider was asked to unprovision, in the order the calls came in.
   *
   * @return the recorded unprovision contexts
   */
  public static List<TableLocationContext> unprovisioned() {
    return Lists.newArrayList(UNPROVISIONED);
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

  /** Clears the recorded unprovision calls and stops unprovisioning from failing. */
  public static void reset() {
    UNPROVISIONED.clear();
    failOnUnprovision = false;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.catalogProperties = catalogProperties;
  }

  @Override
  public String provisionTableLocation(TableLocationContext context) {
    return LOCATION_PREFIX + context.tableIdentifier().name() + "/";
  }

  @Override
  public void unprovisionTableLocation(TableLocationContext context) {
    UNPROVISIONED.add(context);
    if (failOnUnprovision) {
      throw new IllegalStateException("The path allocation service is unavailable");
    }
  }

  @Override
  public void close() {
    this.closed = true;
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
