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
package org.apache.gravitino.encryption.kms.azure;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

final class AzureKeyVaultKeyIdentifier {

  private static final Pattern KEY_NAME = Pattern.compile("[A-Za-z0-9-]{1,127}");
  private static final Pattern KEY_VERSION = Pattern.compile("[A-Za-z0-9-]+");

  private final String name;
  @Nullable private final String version;

  private AzureKeyVaultKeyIdentifier(String name, @Nullable String version) {
    this.name = name;
    this.version = version;
  }

  static AzureKeyVaultKeyIdentifier parse(String keyId, URI vaultUri) {
    URI keyUri;
    try {
      keyUri = new URI(keyId);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Azure Key Vault key ID must be a valid URI", e);
    }

    validateAbsoluteKeyUri(keyUri);
    if (!sameOrigin(vaultUri, keyUri)) {
      throw new IllegalArgumentException(
          String.format("Azure Key Vault key ID must belong to configured vault '%s'", vaultUri));
    }

    String[] pathSegments = keyUri.getRawPath().split("/", -1);
    if ((pathSegments.length != 3 && pathSegments.length != 4)
        || !pathSegments[0].isEmpty()
        || !"keys".equalsIgnoreCase(pathSegments[1])
        || !KEY_NAME.matcher(pathSegments[2]).matches()
        || (pathSegments.length == 4 && !KEY_VERSION.matcher(pathSegments[3]).matches())) {
      throw new IllegalArgumentException(
          "Azure Key Vault key ID must have the form https://<vault>/keys/<name>[/<version>]");
    }

    return new AzureKeyVaultKeyIdentifier(
        pathSegments[2], pathSegments.length == 4 ? pathSegments[3] : null);
  }

  static URI validateVaultUri(URI vaultUri) {
    if (vaultUri == null
        || !vaultUri.isAbsolute()
        || !"https".equalsIgnoreCase(vaultUri.getScheme())
        || vaultUri.getHost() == null
        || vaultUri.getUserInfo() != null
        || vaultUri.getPort() != -1
        || vaultUri.getRawQuery() != null
        || vaultUri.getRawFragment() != null
        || !(vaultUri.getRawPath().isEmpty() || "/".equals(vaultUri.getRawPath()))) {
      throw new IllegalArgumentException(
          "Azure Key Vault URI must be an HTTPS origin without credentials, a port, query, or fragment");
    }
    return vaultUri;
  }

  String name() {
    return name;
  }

  @Nullable
  String version() {
    return version;
  }

  private static void validateAbsoluteKeyUri(URI keyUri) {
    if (!keyUri.isAbsolute()
        || !"https".equalsIgnoreCase(keyUri.getScheme())
        || keyUri.getHost() == null
        || keyUri.getUserInfo() != null
        || keyUri.getPort() != -1
        || keyUri.getRawQuery() != null
        || keyUri.getRawFragment() != null) {
      throw new IllegalArgumentException(
          "Azure Key Vault key ID must be an HTTPS URL without credentials, a port, query, or fragment");
    }
  }

  private static boolean sameOrigin(URI first, URI second) {
    return first
            .getScheme()
            .toLowerCase(Locale.ROOT)
            .equals(second.getScheme().toLowerCase(Locale.ROOT))
        && first.getHost().equalsIgnoreCase(second.getHost())
        && first.getPort() == second.getPort();
  }
}
