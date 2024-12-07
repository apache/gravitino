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

/** ADLS SAS token credential. */
public class ADLSTokenCredential implements Credential {

  /** ADLS SAS token credential type. */
  private static final String ADLS_SAS_TOKEN_CREDENTIAL_TYPE = "adls-sas-token";
  /** ADLS SAS token used to access ADLS data. */
  public static final String GRAVITINO_ADLS_SAS_TOKEN = "adls-sas-token";

  private final String sasToken;
  private final long expireTimeInMS;

  /**
   * Constructs an instance of {@link ADLSTokenCredential} with SAS token.
   *
   * @param sasToken The ADLS SAS token.
   * @param expireTimeInMS The SAS token expire time in ms.
   */
  public ADLSTokenCredential(String sasToken, long expireTimeInMS) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sasToken), "ADLS SAS token should not be empty");

    this.sasToken = sasToken;
    this.expireTimeInMS = expireTimeInMS;
  }

  @Override
  public String credentialType() {
    return ADLS_SAS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return expireTimeInMS;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GRAVITINO_ADLS_SAS_TOKEN, sasToken)
        .build();
  }

  /**
   * Get ADLS SAS token.
   *
   * @return The ADLS SAS token.
   */
  public String sasToken() {
    return sasToken;
  }
}
