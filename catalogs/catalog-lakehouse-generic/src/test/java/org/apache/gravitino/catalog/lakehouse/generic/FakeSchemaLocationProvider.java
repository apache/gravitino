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
import org.apache.gravitino.rel.Table;

/**
 * A {@link TableLocationProvider} that also implements {@link SupportsSchemaLocations}, registered
 * only in the test classpath, used to verify that the generic catalog consults it for schemas.
 */
public class FakeSchemaLocationProvider implements TableLocationProvider, SupportsSchemaLocations {

  /** The name under which this provider is registered. */
  public static final String NAME = "testing-schema";

  /** The prefix of every schema location allocated by this provider. */
  public static final String LOCATION_PREFIX = "testing://schema-bucket/";

  // The provider instance used by a catalog is created by the ServiceLoader and not reachable from
  // the test, so the calls are recorded statically.
  private static final List<SchemaLocationContext> PROVISIONED =
      Collections.synchronizedList(Lists.newArrayList());

  private static final List<SchemaLocationContext> UNPROVISIONED =
      Collections.synchronizedList(Lists.newArrayList());

  private static volatile boolean overrideLocation;

  private static volatile String locationOverride;

  private static volatile boolean failOnProvision;

  private static volatile boolean failOnUnprovision;

  /** Public and no-argument, as the SPI requires; it acquires nothing. */
  public FakeSchemaLocationProvider() {}

  /**
   * Returns the contexts this provider was asked to provision a schema location for.
   *
   * @return the recorded provision contexts
   */
  public static List<SchemaLocationContext> provisioned() {
    return Lists.newArrayList(PROVISIONED);
  }

  /**
   * Returns the contexts this provider was asked to unprovision a schema location for.
   *
   * @return the recorded unprovision contexts
   */
  public static List<SchemaLocationContext> unprovisioned() {
    return Lists.newArrayList(UNPROVISIONED);
  }

  /**
   * Makes every subsequent schema provision call return the given location, null included.
   *
   * @param location the location to return
   */
  public static void provisionLocation(String location) {
    overrideLocation = true;
    locationOverride = location;
  }

  /**
   * Makes every subsequent schema provision call fail.
   *
   * @param fail whether provisioning should throw
   */
  public static void failOnProvision(boolean fail) {
    failOnProvision = fail;
  }

  /**
   * Makes every subsequent schema unprovision call fail.
   *
   * @param fail whether unprovisioning should throw
   */
  public static void failOnUnprovision(boolean fail) {
    failOnUnprovision = fail;
  }

  /** Clears the recorded calls and the configured behaviour. */
  public static void reset() {
    PROVISIONED.clear();
    UNPROVISIONED.clear();
    overrideLocation = false;
    locationOverride = null;
    failOnProvision = false;
    failOnUnprovision = false;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String provisionTableLocation(TableLocationContext context) {
    return context.schema().properties().get(Table.PROPERTY_LOCATION)
        + context.tableIdentifier().name()
        + "/";
  }

  @Override
  public void unprovisionTableLocation(TableLocationContext context) {}

  @Override
  public String provisionSchemaLocation(SchemaLocationContext context) {
    PROVISIONED.add(context);
    if (failOnProvision) {
      throw new IllegalStateException("The path allocation service is unavailable");
    }
    return overrideLocation
        ? locationOverride
        : LOCATION_PREFIX + context.schemaIdentifier().name() + "/";
  }

  @Override
  public void unprovisionSchemaLocation(SchemaLocationContext context) {
    UNPROVISIONED.add(context);
    if (failOnUnprovision) {
      throw new IllegalStateException("The path allocation service is unavailable");
    }
  }
}
