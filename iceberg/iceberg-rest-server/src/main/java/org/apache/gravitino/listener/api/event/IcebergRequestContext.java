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

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * The general request context information for Iceberg REST operations.
 *
 * <p>Optional {@link #auditExtras()} are facts an inner dispatcher wants on the terminal Iceberg
 * event. They are not HTTP headers and never appear in {@link #httpHeaders()}.
 */
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
  private final Map<String, String> auditExtras;

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
    this(
        httpRequest,
        catalogName,
        PrincipalUtils.getCurrentUserName(),
        resolveClientAddress(httpRequest),
        IcebergRESTUtils.getHttpHeaders(httpRequest),
        requestCredentialVending,
        ImmutableMap.of());
  }

  private IcebergRequestContext(
      HttpServletRequest httpServletRequest,
      String catalogName,
      String userName,
      String remoteHostName,
      Map<String, String> httpHeaders,
      boolean requestCredentialVending,
      Map<String, String> auditExtras) {
    this.httpServletRequest = httpServletRequest;
    this.catalogName = catalogName;
    this.userName = userName;
    this.remoteHostName = remoteHostName;
    this.httpHeaders = httpHeaders;
    this.requestCredentialVending = requestCredentialVending;
    this.auditExtras = auditExtras;
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
   * Returns the immutable audit extras attached to this context. Empty when none were attached.
   *
   * @return audit extras, never {@code null}
   */
  public Map<String, String> auditExtras() {
    return auditExtras;
  }

  /**
   * Returns a copy of this context with the given audit extras. Extras are not part of {@link
   * #httpHeaders()}. A {@code null} or empty map yields a context with empty extras.
   *
   * @param extras optional facts for {@code customInfo()}, or {@code null} for none
   * @return a new context; this instance is unchanged
   */
  public IcebergRequestContext withAuditExtras(Map<String, String> extras) {
    return new IcebergRequestContext(
        httpServletRequest,
        catalogName,
        userName,
        remoteHostName,
        httpHeaders,
        requestCredentialVending,
        copyAuditExtras(extras));
  }

  /**
   * Returns HTTP headers merged with {@link #auditExtras()}. When extras are empty, returns {@link
   * #httpHeaders()} unchanged so existing callers keep today's header-only map. Extra keys overlay
   * headers on conflict.
   *
   * @return headers, or headers overlaid with extras
   */
  public Map<String, String> customInfo() {
    if (auditExtras.isEmpty()) {
      return httpHeaders;
    }
    Map<String, String> merged = new LinkedHashMap<>(httpHeaders);
    merged.putAll(auditExtras);
    return ImmutableMap.copyOf(merged);
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

  private static Map<String, String> copyAuditExtras(Map<String, String> extras) {
    if (extras == null || extras.isEmpty()) {
      return ImmutableMap.of();
    }
    return ImmutableMap.copyOf(extras);
  }
}
