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

package org.apache.gravitino.listener.api.event;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;

/** The general request context information for Iceberg REST operations. */
@DeveloperApi
public class IcebergRequestContext {

  /** Header that opts a drop purge request into asynchronous file cleanup. */
  public static final String ASYNC_PURGE_HEADER = "X-Gravitino-Async-Purge";

  private final String catalogName;
  private final String userName;
  private final String remoteHostName;
  private final Map<String, String> httpHeaders;
  private final boolean requestCredentialVending;

  /**
   * Constructs a new {@code IcebergRequestContext} instance.
   *
   * @param catalogName The name of the catalog to be accessed in the request.
   * @param userName The username of the HTTP client.
   * @param remoteHostName The hostname of the HTTP client.
   * @param httpHeaders The HTTP request headers.
   * @param requestCredentialVending Whether the request is for credential vending.
   */
  public IcebergRequestContext(
      String catalogName,
      String userName,
      String remoteHostName,
      Map<String, String> httpHeaders,
      boolean requestCredentialVending) {
    Preconditions.checkArgument(catalogName != null, "catalogName cannot be null");
    Preconditions.checkArgument(userName != null, "userName cannot be null");
    Preconditions.checkArgument(remoteHostName != null, "remoteHostName cannot be null");
    Preconditions.checkArgument(httpHeaders != null, "httpHeaders cannot be null");
    this.catalogName = catalogName;
    this.userName = userName;
    this.remoteHostName = remoteHostName;
    this.httpHeaders = Collections.unmodifiableMap(new HashMap<>(httpHeaders));
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
}
