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

package org.apache.gravitino.credential;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Interface representing a credential with type, expiration time, and additional information. */
public interface Credential {
  /** Credential type in the credential. */
  String CREDENTIAL_TYPE = "credential-type";
  /** Credential expire time in ms since the epoch. */
  String EXPIRE_TIME_IN_MS = "expire-time-in-ms";

  /**
   * Returns the type of the credential. It should be the same as the credential type of the
   * credential provider.
   *
   * @return the credential type as a String.
   */
  String credentialType();

  /**
   * Returns the expiration time of the credential in milliseconds since the epoch, 0 means not
   * expire.
   *
   * @return the expiration time as a long.
   */
  long expireTimeInMs();

  /**
   * Returns credential information.
   *
   * @return a map of credential information.
   */
  Map<String, String> credentialInfo();

  /**
   * Converts the credential to properties to transfer the credential though API.
   *
   * @return a map containing credential properties.
   */
  default Map<String, String> toProperties() {
    return new ImmutableMap.Builder<String, String>()
        .putAll(credentialInfo())
        .put(CREDENTIAL_TYPE, credentialType())
        .put(EXPIRE_TIME_IN_MS, String.valueOf(expireTimeInMs()))
        .build();
  }
}
