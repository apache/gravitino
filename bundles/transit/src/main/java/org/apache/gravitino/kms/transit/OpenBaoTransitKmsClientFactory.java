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
package org.apache.gravitino.kms.transit;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.KmsConfigurationException;

/** Creates clients for the OpenBao Transit secrets engine. */
@DeveloperApi
public final class OpenBaoTransitKmsClientFactory implements KmsClientFactory {

  /** Property containing the OpenBao HTTP(S) server base address. */
  public static final String SERVICE_ADDRESS = "endpoint.address";

  /** Property containing the OpenBao Transit mount path. */
  public static final String TRANSIT_MOUNT = "endpoint.transitMount";

  /** Property selecting token-file authentication. */
  public static final String CREDENTIAL_METHOD = "credential.method";

  /** Property containing an absolute path to an OpenBao token file. */
  public static final String TOKEN_FILE = "credential.path";

  /** Default OpenBao Transit mount path. */
  public static final String DEFAULT_TRANSIT_MOUNT = "transit";

  private static final Set<String> SUPPORTED_PROPERTIES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(SERVICE_ADDRESS, TRANSIT_MOUNT, CREDENTIAL_METHOD, TOKEN_FILE)));

  /** {@inheritDoc} */
  @Override
  public KmsApi api() {
    return KmsApi.OPENBAO_TRANSIT;
  }

  /** {@inheritDoc} */
  @Override
  public KmsClient create(String source, Map<String, String> properties) {
    validateSource(source);
    if (properties == null) {
      throw new KmsConfigurationException("OpenBao Transit properties cannot be null");
    }
    for (String property : properties.keySet()) {
      if (!SUPPORTED_PROPERTIES.contains(property)) {
        throw new KmsConfigurationException("Unsupported OpenBao Transit property: %s", property);
      }
    }

    URI serviceAddress = parseServiceAddress(requireProperty(properties, SERVICE_ADDRESS));
    String credentialMethod = requireProperty(properties, CREDENTIAL_METHOD);
    if (!"token_file".equals(credentialMethod)) {
      throw new KmsConfigurationException(
          "Unsupported OpenBao Transit credential method: %s", credentialMethod);
    }
    String transitMount = properties.get(TRANSIT_MOUNT);
    if (transitMount == null) {
      transitMount = DEFAULT_TRANSIT_MOUNT;
    } else {
      transitMount = transitMount.trim();
    }
    validateTransitMount(transitMount);
    Path tokenFile = parseTokenFile(requireProperty(properties, TOKEN_FILE));

    return new OpenBaoTransitKmsClient(source, serviceAddress, transitMount, tokenFile);
  }

  private static String requireProperty(Map<String, String> properties, String name) {
    String value = properties.get(name);
    if (value == null || value.trim().isEmpty()) {
      throw new KmsConfigurationException("Missing required OpenBao Transit property: %s", name);
    }
    return value.trim();
  }

  private static URI parseServiceAddress(String value) {
    URI uri;
    try {
      uri = URI.create(value);
    } catch (IllegalArgumentException e) {
      throw new KmsConfigurationException(e, "Invalid OpenBao Transit endpoint address");
    }

    String path = uri.getPath();
    if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || uri.getQuery() != null
        || uri.getFragment() != null
        || !(path == null || path.isEmpty() || "/".equals(path))) {
      throw new KmsConfigurationException(
          "OpenBao Transit endpoint address must be an HTTP(S) server base address");
    }

    String address = uri.toString();
    return URI.create(address.endsWith("/") ? address.substring(0, address.length() - 1) : address);
  }

  private static Path parseTokenFile(String value) {
    Path path;
    try {
      path = Paths.get(value);
    } catch (RuntimeException e) {
      throw new KmsConfigurationException(e, "Invalid OpenBao Transit credential path");
    }
    if (!path.isAbsolute()) {
      throw new KmsConfigurationException(
          "OpenBao Transit credential path must be an absolute path");
    }
    return path;
  }

  private static void validateSource(String source) {
    if (source == null || source.trim().isEmpty()) {
      throw new KmsConfigurationException("OpenBao Transit source cannot be blank");
    }
  }

  private static void validateTransitMount(String mount) {
    if (mount.isEmpty() || mount.startsWith("/") || mount.endsWith("/")) {
      throw new KmsConfigurationException("Invalid OpenBao Transit mount path");
    }
    for (String segment : mount.split("/", -1)) {
      if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
        throw new KmsConfigurationException("Invalid OpenBao Transit mount path");
      }
    }
  }
}
