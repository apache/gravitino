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
package org.apache.gravitino.iceberg.service.sign;

import java.net.URI;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.exceptions.ForbiddenException;

/** Validates that a remote-sign URI is within allowed table storage prefixes. */
public final class RemoteSignPathValidator {

  private RemoteSignPathValidator() {}

  /**
   * Validates that the request URI resolves to an object under one of the allowed prefixes.
   *
   * @param requestUri URI from the remote sign request
   * @param allowedPrefixes table location prefixes, such as {@code s3://bucket/table}
   * @throws ForbiddenException if the URI is outside the allowed prefixes
   */
  public static void validateUriWithinPrefixes(URI requestUri, Set<String> allowedPrefixes) {
    String requestLocation = toStorageLocation(requestUri);
    for (String prefix : allowedPrefixes) {
      if (StringUtils.isBlank(prefix)) {
        continue;
      }
      String normalizedPrefix = normalizePrefix(prefix);
      if (requestLocation.equals(normalizedPrefix)
          || requestLocation.startsWith(normalizedPrefix + "/")) {
        return;
      }
    }
    throw new ForbiddenException(
        "Remote sign URI %s is outside allowed table locations: %s", requestUri, allowedPrefixes);
  }

  /**
   * Converts a request URI to a normalized storage location string.
   *
   * @param uri request URI, either {@code s3://} or HTTPS S3 endpoint form
   * @return normalized location in {@code s3://bucket/key} form without a trailing slash
   */
  public static String toStorageLocation(URI uri) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("URI scheme is required: " + uri);
    }

    if ("s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme)) {
      return normalizeS3Uri(uri.getHost(), uri.getPath());
    }

    if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
      return normalizeHttpS3Uri(uri);
    }

    throw new IllegalArgumentException("Unsupported URI scheme for remote signing: " + scheme);
  }

  private static String normalizeHttpS3Uri(URI uri) {
    String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("URI host is required: " + uri);
    }

    String path = uri.getPath() == null ? "" : uri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    int s3Index = host.indexOf(".s3");
    if (s3Index > 0) {
      String bucket = host.substring(0, s3Index);
      return normalizeS3Uri(bucket, path.isEmpty() ? "" : "/" + path);
    }

    if (host.startsWith("s3.") || "s3.amazonaws.com".equals(host)) {
      int slash = path.indexOf('/');
      if (slash < 0) {
        throw new IllegalArgumentException("Path-style S3 URI must include a key: " + uri);
      }
      String bucket = path.substring(0, slash);
      String key = path.substring(slash);
      return normalizeS3Uri(bucket, key);
    }

    throw new IllegalArgumentException("Unrecognized S3 HTTP endpoint: " + host);
  }

  private static String normalizeS3Uri(String bucket, String path) {
    if (StringUtils.isBlank(bucket)) {
      throw new IllegalArgumentException("S3 bucket is required");
    }
    String key = path == null ? "" : path;
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    if (key.isEmpty()) {
      return "s3://" + bucket;
    }
    return "s3://" + bucket + "/" + key;
  }

  private static String normalizePrefix(String prefix) {
    String normalized = prefix.trim();
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }
}
