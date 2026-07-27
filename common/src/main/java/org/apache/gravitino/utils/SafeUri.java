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
package org.apache.gravitino.utils;

import java.net.URI;

/** Renders URIs for logs and error messages without their credential-bearing parts. */
final class SafeUri {

  private SafeUri() {}

  /**
   * Returns a log-safe rendering of {@code uri} with the userinfo (e.g. {@code user:password@}) and
   * query string (which may carry presigned-URL tokens) removed. The scheme, host, port and path
   * are kept so the message stays useful for diagnosis.
   *
   * @param uri the URI to redact; may be {@code null}
   * @return a redacted string safe to log
   */
  static String redact(URI uri) {
    if (uri == null) {
      return "null";
    }
    StringBuilder builder = new StringBuilder();
    if (uri.getScheme() != null) {
      builder.append(uri.getScheme()).append("://");
    }
    if (uri.getHost() != null) {
      builder.append(uri.getHost());
      if (uri.getPort() != -1) {
        builder.append(':').append(uri.getPort());
      }
    }
    if (uri.getRawPath() != null) {
      builder.append(uri.getRawPath());
    }
    return builder.length() == 0 ? "<redacted uri>" : builder.toString();
  }
}
