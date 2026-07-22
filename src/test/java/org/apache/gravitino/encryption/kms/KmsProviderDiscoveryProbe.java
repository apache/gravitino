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
package org.apache.gravitino.encryption.kms;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.ServiceLoader;
import java.util.Set;

/** Verifies KMS provider discovery from the packaged Gravitino runtime classpath. */
final class KmsProviderDiscoveryProbe {

  private static final Set<KmsApi> EXPECTED_APIS = EnumSet.allOf(KmsApi.class);

  private KmsProviderDiscoveryProbe() {}

  /**
   * Loads each packaged provider without creating a client or contacting a KMS.
   *
   * @param args the packaged KMS provider directory and generated server launcher
   * @throws IOException if the generated launcher cannot be read
   * @throws URISyntaxException if a provider's code source is not a valid URI
   */
  public static void main(String[] args) throws IOException, URISyntaxException {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "Expected the packaged KMS provider directory and generated server launcher");
    }

    Path providerDirectory = Paths.get(args[0]).toAbsolutePath().normalize();
    verifyLauncher(Paths.get(args[1]));
    Set<KmsApi> discoveredApis = EnumSet.noneOf(KmsApi.class);
    int factoryCount = 0;
    for (KmsClientFactory factory : ServiceLoader.load(KmsClientFactory.class)) {
      verifyCodeSource(factory, providerDirectory);
      if (!discoveredApis.add(factory.api())) {
        throw new AssertionError(
            String.format("Duplicate packaged KMS factory for API %s", factory.api()));
      }
      factoryCount++;
    }

    if (factoryCount != EXPECTED_APIS.size() || !discoveredApis.equals(EXPECTED_APIS)) {
      throw new AssertionError(
          String.format(
              "Expected %d packaged KMS factories for %s, but found %d for %s",
              EXPECTED_APIS.size(), EXPECTED_APIS, factoryCount, discoveredApis));
    }
  }

  private static void verifyLauncher(Path launcher) throws IOException {
    String launcherContent = Files.readString(launcher);
    String expectedEntry = "addJarInDir \"${GRAVITINO_HOME}/kms-providers\"";
    if (!launcherContent.contains(expectedEntry)) {
      throw new AssertionError(
          String.format("Generated server launcher %s does not load KMS providers", launcher));
    }
  }

  private static void verifyCodeSource(KmsClientFactory factory, Path providerDirectory)
      throws URISyntaxException {
    Path codeSource =
        Paths.get(factory.getClass().getProtectionDomain().getCodeSource().getLocation().toURI())
            .toAbsolutePath()
            .normalize();
    if (!providerDirectory.equals(codeSource.getParent())) {
      throw new AssertionError(
          String.format(
              "KMS factory %s was loaded from %s instead of %s",
              factory.getClass().getName(), codeSource, providerDirectory));
    }
  }
}
