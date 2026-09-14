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

package org.apache.gravitino.client;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;

/** The provider of authentication data */
interface AuthDataProvider extends Closeable {

  /**
   * Judge whether AuthDataProvider can provide token data.
   *
   * @return true if the AuthDataProvider can provide token data otherwise false.
   */
  default boolean hasTokenData() {
    return false;
  }

  /**
   * Returns additional authentication headers for the current request.
   *
   * <p>Called on the requesting thread for every HTTP request. Implementations must not retain
   * caller-specific state in a shared provider. The token still supplies the Authorization header.
   *
   * <p>These headers are applied after the ones passed to the request, so a name returned here
   * replaces a caller-supplied header of the same name. Authorization is set afterwards and cannot
   * be overridden from here.
   *
   * @return additional headers, or an empty map
   */
  default Map<String, String> getRequestHeaders() {
    return Collections.emptyMap();
  }

  /**
   * Acquire the data of token for authentication. The client will set the token data as HTTP header
   * Authorization directly. So the return value should ensure token data contain the token header
   * (eg: Bearer, Basic) if necessary.
   *
   * @return the token data is used for authentication.
   */
  default byte[] getTokenData() {
    return null;
  }
}
