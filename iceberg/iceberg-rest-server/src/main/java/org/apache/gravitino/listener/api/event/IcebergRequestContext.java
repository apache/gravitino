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

package org.apache.gravitino.listener.api.event;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.utils.PrincipalUtils;

/** The general request context information for Iceberg REST operations. */
public class IcebergRequestContext {

  /** Header that opts a drop purge request into asynchronous file cleanup. */
  public static final String ASYNC_PURGE_HEADER = "X-Gravitino-Async-Purge";

  /**
   * @deprecated Kept only for backward-compatibility and will be removed in the next major release.
   */
  @Deprecated private final HttpServletRequest httpServletRequest;

  private final String catalogName;
  private final String userName;
  private final String remoteHostName;
  private final Map<String, String> httpHeaders;
  private final boolean requestCredentialVending;

  /**
   * Constructs a new {@code IcebergRequestContext} instance.
   *
   * @param httpRequest The HttpServletRequest object containing request details.
   * @param catalogName The name of the catalog to be accessed in the request.
   */
  public IcebergRequestContext(HttpServletRequest httpRequest, String catalogName) {
    this(httpRequest, catalogName, false);
  }

  /**
   * Constructs a new {@code IcebergRequestContext} instance.
   *
   * @param httpRequest The HttpServletRequest object containing request details.
   * @param catalogName The name of the catalog to be accessed in the request.
   * @param requestCredentialVending Whether the request is for credential vending.
   */
  public IcebergRequestContext(
      HttpServletRequest httpRequest, String catalogName, boolean requestCredentialVending) {
    this.httpServletRequest = httpRequest;
    this.remoteHostName = resolveClientAddress(httpRequest);
    this.httpHeaders = IcebergRESTUtils.getHttpHeaders(httpRequest);
    this.catalogName = catalogName;
    this.userName = PrincipalUtils.getCurrentUserName();
    this.requestCredentialVending = requestCredentialVending;
  }

  private static String resolveClientAddress(HttpServletRequest request) {
    // X-Forwarded-For is trusted unconditionally; callers in environments where the server is
    // reachable directly (not only via a trusted proxy) should be aware that this header can be
    // spoofed by clients.
    String xForwardedFor = request.getHeader("X-Forwarded-For");
    if (StringUtils.isNotBlank(xForwardedFor)) {
      return xForwardedFor.split(",")[0].trim();
    }
    return request.getRemoteHost();
  }

  /**
   * Returns the catalog name.
   *
   * @return The catalog name.
   */
  public String catalogName() {
    return catalogName;
  }

  /**
   * Returns the username of the HTTP client.
   *
   * @return The username.
   */
  public String userName() {
    return userName;
  }

  /**
   * Returns the hostname of the HTTP client.
   *
   * @return The remote host name.
   */
  public String remoteHostName() {
    return remoteHostName;
  }

  /**
   * Returns the Map of the HTTP headers.
   *
   * @return The HTTP header.
   */
  public Map<String, String> httpHeaders() {
    return httpHeaders;
  }

  /**
   * Checks whether this request opted into asynchronous table purge.
   *
   * <p>Async purge is opt-in. Standard Iceberg clients send no header and keep synchronous purge
   * behavior; a client opts in with {@code X-Gravitino-Async-Purge: true}.
   *
   * @return true only when the async purge header explicitly says {@code true}
   */
  public boolean asyncPurge() {
    for (Map.Entry<String, String> header : httpHeaders.entrySet()) {
      // HTTP header names are case-insensitive; the value is matched exactly as "true".
      if (ASYNC_PURGE_HEADER.equalsIgnoreCase(header.getKey())) {
        return "true".equals(header.getValue().trim());
      }
    }
    return false;
  }

  /**
   * Checks if the request is for credential vending.
   *
   * @return true if the request is for credential vending, false otherwise.
   */
  public boolean requestCredentialVending() {
    return requestCredentialVending;
  }

  /**
   * Retrieves the HttpServletRequest object. This method is deprecated and should be used
   * cautiously.
   *
   * @return The HttpServletRequest associated with this context.
   * @deprecated Use other methods to access specific request details instead.
   */
  @Deprecated
  public HttpServletRequest getHttpServletRequest() {
    return httpServletRequest;
  }
}
