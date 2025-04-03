/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.http;

/**
 * Used to store the user's identity information when requesting a REST API, and allows the user
 * information to be accessed anywhere within the request thread.
 */
public class RequestContextHolder {

  private static final RequestContextHolder INSTANCE = new RequestContextHolder();

  private RequestContextHolder() {}

  /**
   * Store the user's identity information when requesting a REST API
   *
   * @param requestContext a request context include userid
   */
  public void setRequestContext(RequestContext requestContext) {
    // TODO
  }

  /**
   * Retrieve the RequestContext from anywhere within the thread.
   *
   * @return RequestContext
   */
  public RequestContext getRequestContext() {
    // TODO
    return null;
  }

  /** Called when the HTTP request ends to prevent memory leaks. */
  public void remove() {
    // TODO
  }
}
