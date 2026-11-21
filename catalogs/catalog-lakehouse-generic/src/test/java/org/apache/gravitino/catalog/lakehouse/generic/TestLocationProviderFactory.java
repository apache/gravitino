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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLocationProviderFactory {

  @Test
  void testCreateDefaultProvider() {
    LocationProvider provider = create(DefaultLocationProvider.NAME);
    Assertions.assertInstanceOf(DefaultLocationProvider.class, provider);
    Assertions.assertEquals(DefaultLocationProvider.NAME, provider.name());
  }

  @Test
  void testProviderNameIsCaseInsensitive() {
    Assertions.assertInstanceOf(DefaultLocationProvider.class, create("DeFaUlT"));
  }

  @Test
  void testCreateCustomProviderDiscoveredViaServiceLoader() {
    Assertions.assertInstanceOf(FakeLocationProvider.class, create(FakeLocationProvider.NAME));
  }

  @Test
  void testEachCallReturnsANewInstance() {
    Assertions.assertNotSame(
        create(DefaultLocationProvider.NAME), create(DefaultLocationProvider.NAME));
  }

  @Test
  void testTheFactoryNeitherInitializesNorClosesAnything() {
    // The interface has no lifecycle callbacks, so a provider is handed back exactly as its
    // constructor left it. Pinned because the factory used to initialize it here, and a provider
    // written against that behaviour would otherwise fail in a way nothing else in the module
    // catches: the instance it gets is the instance it keeps.
    FakeLocationProvider.reset();
    LocationProvider provider = create(FakeLocationProvider.NAME);

    Assertions.assertInstanceOf(FakeLocationProvider.class, provider);
    Assertions.assertTrue(
        FakeLocationProvider.provisioned().isEmpty(),
        "creating a provider must not call anything on it");
  }

  @Test
  void testABrokenProviderOnTheClasspathIsSkippedRatherThanFailingTheLookup() {
    // BrokenLocationProvider is registered in this module's test services file, so it is
    // instantiated and asked for its name on every lookup here, including this one. That it fails
    // with an Error rather than an exception is the point: name() is called by the factory and not
    // by the loader, so nothing wraps it, and a catch that only took RuntimeException would let it
    // through and stop every catalog from starting over one broken jar.
    Assertions.assertInstanceOf(
        DefaultLocationProvider.class, create(DefaultLocationProvider.NAME));

    // The name it never managed to report is still not a name anyone can select.
    Assertions.assertThrows(IllegalArgumentException.class, () -> create("broken"));
  }

  @Test
  void testUnknownProviderThrows() {
    IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, () -> create("no-such-provider"));
    Assertions.assertTrue(
        e.getMessage().contains("No LocationProvider found for name 'no-such-provider'"),
        "Unexpected message: " + e.getMessage());
  }

  @Test
  void testBlankProviderNameThrows() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> create(" "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> create(null));
  }

  @Test
  void testEveryLookupScansRatherThanAnsweringFromACache() {
    FakeLocationProvider.reset();

    create(DefaultLocationProvider.NAME);
    int afterFirstLookup = FakeLocationProvider.constructedCount();
    Assertions.assertTrue(afterFirstLookup >= 1, "selection has to construct every candidate");

    create(DefaultLocationProvider.NAME);

    // Discovery is repeated per catalog rather than remembered. Pinned because it is a deliberate
    // choice and not an oversight: a provider registered after the first catalog started is found
    // by the next one, and nothing static outlives the lookup to be invalidated later.
    Assertions.assertTrue(
        FakeLocationProvider.constructedCount() > afterFirstLookup,
        "a second lookup should scan again rather than answer from a remembered index");
  }

  private static LocationProvider create(String name) {
    return LocationProviderFactory.create(name);
  }
}
