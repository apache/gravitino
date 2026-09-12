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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestTableLocationProviderFactory {

  private static final Map<String, String> CATALOG_PROPERTIES =
      ImmutableMap.of("location", "/tmp/catalog");

  @BeforeEach
  void forgetDiscoveredProviders() {
    // The index is remembered per class loader and every test here shares one, so without this
    // only the first test would exercise a real ServiceLoader scan and the rest would be reading
    // back what it found.
    TableLocationProviderFactory.invalidateCache();
  }

  @Test
  void testCreateDefaultProvider() throws IOException {
    try (TableLocationProvider provider = create(DefaultTableLocationProvider.NAME)) {
      Assertions.assertInstanceOf(DefaultTableLocationProvider.class, provider);
      Assertions.assertEquals(DefaultTableLocationProvider.NAME, provider.name());
    }
  }

  @Test
  void testProviderNameIsCaseInsensitive() throws IOException {
    try (TableLocationProvider provider = create("DeFaUlT")) {
      Assertions.assertInstanceOf(DefaultTableLocationProvider.class, provider);
    }
  }

  @Test
  void testCreateCustomProviderDiscoveredViaServiceLoader() throws IOException {
    try (TableLocationProvider provider = create(FakeTableLocationProvider.NAME)) {
      FakeTableLocationProvider fake =
          Assertions.assertInstanceOf(FakeTableLocationProvider.class, provider);
      // The provider is initialized with the catalog properties before it is handed back.
      Assertions.assertEquals(CATALOG_PROPERTIES, fake.catalogProperties());
      Assertions.assertFalse(fake.isClosed());
    }
  }

  @Test
  void testEachCallReturnsANewInstance() throws IOException {
    try (TableLocationProvider first = create(DefaultTableLocationProvider.NAME);
        TableLocationProvider second = create(DefaultTableLocationProvider.NAME)) {
      Assertions.assertNotSame(first, second);
    }
  }

  @Test
  void testProviderIsClosedWhenInitializeThrows() {
    FakeTableLocationProvider.reset();
    FakeTableLocationProvider.failOnInitialize(true);
    try {
      Assertions.assertThrows(
          IllegalStateException.class, () -> create(FakeTableLocationProvider.NAME));

      // A provider that failed halfway through initialize holds whatever it managed to acquire,
      // and never reaches the caller that would have owned its lifecycle, so the factory has to
      // close it.
      Assertions.assertEquals(1, FakeTableLocationProvider.closedCount());
    } finally {
      FakeTableLocationProvider.reset();
    }
  }

  @Test
  void testABrokenProviderOnTheClasspathIsSkippedRatherThanFailingTheLookup() throws IOException {
    // BrokenTableLocationProvider is registered in this module's test services file, so it is
    // instantiated and asked for its name on every lookup here, including this one. That it fails
    // with an Error rather than an exception is the point: name() is called by the factory and not
    // by the loader, so nothing wraps it, and a catch that only took RuntimeException would let it
    // through and stop every catalog from starting over one broken jar.
    try (TableLocationProvider provider = create(DefaultTableLocationProvider.NAME)) {
      Assertions.assertInstanceOf(DefaultTableLocationProvider.class, provider);
    }

    // The name it never managed to report is still not a name anyone can select.
    Assertions.assertThrows(IllegalArgumentException.class, () -> create("broken"));
  }

  @Test
  void testUnknownProviderThrows() {
    IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, () -> create("no-such-provider"));
    Assertions.assertTrue(
        e.getMessage().contains("No TableLocationProvider found for name 'no-such-provider'"),
        "Unexpected message: " + e.getMessage());
  }

  @Test
  void testACatalogPropertyWithANullValueReachesTheProvider() throws IOException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("location", "/tmp/catalog");
    properties.put("a-property-with-no-value", null);

    // Nothing upstream rejects a catalog property whose value is null, so the factory must not be
    // what turns one into a failure to build the provider.
    try (TableLocationProvider provider =
        TableLocationProviderFactory.create(FakeTableLocationProvider.NAME, properties)) {
      FakeTableLocationProvider fake =
          Assertions.assertInstanceOf(FakeTableLocationProvider.class, provider);
      Assertions.assertTrue(fake.catalogProperties().containsKey("a-property-with-no-value"));
      Assertions.assertNull(fake.catalogProperties().get("a-property-with-no-value"));
    }
  }

  @Test
  void testBlankProviderNameThrows() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> create(" "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> create(null));
  }

  @Test
  void testTheIndexIsReusedRatherThanRescanned() throws IOException {
    FakeTableLocationProvider.reset();

    try (TableLocationProvider ignored = create(DefaultTableLocationProvider.NAME)) {
      // The first lookup has to construct every registered provider, including this one, because
      // name() is an instance method and there is no other way to learn what a candidate answers
      // to.
      Assertions.assertTrue(FakeTableLocationProvider.constructedCount() >= 1);
    }
    int afterFirstScan = FakeTableLocationProvider.constructedCount();

    try (TableLocationProvider ignored = create(DefaultTableLocationProvider.NAME)) {
      // The second lookup reads the index, so no candidate other than the selected one is
      // constructed. That is the whole benefit of the cache, and it is the part worth pinning.
      Assertions.assertEquals(afterFirstScan, FakeTableLocationProvider.constructedCount());
    }
  }

  @Test
  void testInvalidatingTheIndexForcesARescan() throws IOException {
    FakeTableLocationProvider.reset();

    try (TableLocationProvider ignored = create(DefaultTableLocationProvider.NAME)) {
      Assertions.assertTrue(FakeTableLocationProvider.constructedCount() >= 1);
    }
    int afterFirstScan = FakeTableLocationProvider.constructedCount();

    TableLocationProviderFactory.invalidateCache();
    try (TableLocationProvider ignored = create(DefaultTableLocationProvider.NAME)) {
      // A dropped index is what a collected class loader leaves behind, and the next lookup has to
      // survive it by scanning again rather than reporting the provider as missing.
      Assertions.assertTrue(FakeTableLocationProvider.constructedCount() > afterFirstScan);
    }
  }

  private static TableLocationProvider create(String name) {
    return TableLocationProviderFactory.create(name, CATALOG_PROPERTIES);
  }
}
