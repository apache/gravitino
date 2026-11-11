/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.cos.credential;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.gravitino.credential.CredentialProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards the SPI wiring in {@code META-INF/services/org.apache.gravitino.credential
 * .CredentialProvider}. If a future change accidentally drops {@link COSTokenProvider} (or {@link
 * COSSecretKeyProvider}) from that file, the Gravitino server would silently return an empty {@code
 * credentials: []} response instead of a proper credential. This test fails loudly at build time so
 * the regression cannot slip past CI.
 */
public class TestCOSCredentialProviderSpi {

  @Test
  void testCredentialProviderSpiRegistersBothCosProviders() {
    Set<Class<?>> loaded = new HashSet<>();
    for (CredentialProvider provider : ServiceLoader.load(CredentialProvider.class)) {
      loaded.add(provider.getClass());
    }

    // We assert containment (not equality) because other bundles on the test classpath may
    // register their own providers via the same SPI file.
    Assertions.assertTrue(
        loaded.contains(COSSecretKeyProvider.class),
        "COSSecretKeyProvider not registered via ServiceLoader; check "
            + "bundles/tencent/src/main/resources/META-INF/services/"
            + "org.apache.gravitino.credential.CredentialProvider. Loaded providers: "
            + loaded);
    Assertions.assertTrue(
        loaded.contains(COSTokenProvider.class),
        "COSTokenProvider not registered via ServiceLoader; missing this line means "
            + "cos-token credential vending will silently return empty credentials at runtime. "
            + "Loaded providers: "
            + loaded);
  }
}
