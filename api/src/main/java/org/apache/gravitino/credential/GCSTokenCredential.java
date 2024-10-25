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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** The GCS token credential to access GCS. */
public class GCSTokenCredential implements Credential {

  /** GCS credential type. */
  public static final String GCS_TOKEN_CREDENTIAL_TYPE = "gcs-token";

  /** GCS credential property, token name. */
  public static final String GCS_TOKEN_NAME = "token";

  private String token;
  private long expireMs;

  /**
   * @param token The GCS token.
   * @param expireMs The GCS token expire time at ms.
   */
  public GCSTokenCredential(String token, long expireMs) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(token), "GCS session token should not be null");
    this.token = token;
    this.expireMs = expireMs;
  }

  @Override
  public String credentialType() {
    return GCS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return expireMs;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>()).put(GCS_TOKEN_NAME, token).build();
  }

  /**
   * Get GCS token.
   *
   * @return The GCS token.
   */
  public String token() {
    return token;
  }
}
