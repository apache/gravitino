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
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.utils.PrincipalUtils;

/** The general request context information for Iceberg REST operations. */
public class IcebergRequestContext {

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
    this.remoteHostName = httpRequest.getRemoteHost();
    this.httpHeaders = IcebergRestUtils.getHttpHeaders(httpRequest);
    this.catalogName = catalogName;
    this.userName = PrincipalUtils.getCurrentUserName();
    this.requestCredentialVending = requestCredentialVending;
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
