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

package org.apache.gravitino.auth;

public class AuthProperties {

  /** The configuration key for the Gravitino client auth type. */
  public static final String GRAVITINO_CLIENT_AUTH_TYPE = "authType";

  public static final String SIMPLE_AUTH_TYPE = "simple";
  public static final String OAUTH2_AUTH_TYPE = "oauth2";
  public static final String KERBEROS_AUTH_TYPE = "kerberos";

  // oauth2
  /** The configuration key for the URI of the default OAuth server. */
  public static final String GRAVITINO_OAUTH2_SERVER_URI = "oauth2.serverUri";

  /** The configuration key for the client credential. */
  public static final String GRAVITINO_OAUTH2_CREDENTIAL = "oauth2.credential";

  /** The configuration key for the path which to get the token. */
  public static final String GRAVITINO_OAUTH2_TOKEN_PATH = "oauth2.tokenPath";

  /** The configuration key for the scope of the token. */
  public static final String GRAVITINO_OAUTH2_SCOPE = "oauth2.scope";

  public static boolean isKerberos(String authType) {
    return KERBEROS_AUTH_TYPE.equalsIgnoreCase(authType);
  }

  public static boolean isOAuth2(String authType) {
    return OAUTH2_AUTH_TYPE.equalsIgnoreCase(authType);
  }

  public static boolean isSimple(String authType) {
    return authType == null || SIMPLE_AUTH_TYPE.equalsIgnoreCase(authType);
  }

  private AuthProperties() {}
}
