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
  public static final String ADLS_SAS_TOKEN_CREDENTIAL_TYPE = "adls-sas-token";
  /** ADLS base domain */
  public static final String ADLS_DOMAIN = "dfs.core.windows.net";
  /** ADLS storage account name */
  public static final String GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME = "azure-storage-account-name";
  /** ADLS SAS token used to access ADLS data. */
  public static final String GRAVITINO_ADLS_SAS_TOKEN = "adls-sas-token";

  private String accountName;
  private String sasToken;
  private long expireTimeInMS;

  /**
   * Constructs an instance of {@link ADLSTokenCredential} with SAS token.
   *
   * @param accountName The ADLS account name.
   * @param sasToken The ADLS SAS token.
   * @param expireTimeInMS The SAS token expire time in ms.
   */
  public ADLSTokenCredential(String accountName, String sasToken, long expireTimeInMS) {
    validate(accountName, sasToken, expireTimeInMS);
    this.accountName = accountName;
    this.sasToken = sasToken;
    this.expireTimeInMS = expireTimeInMS;
  }

  /**
   * This is the constructor that is used by credential factory to create an instance of credential
   * according to the credential information.
   */
  public ADLSTokenCredential() {}

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

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMS) {
    String accountName = credentialInfo.get(GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
    String sasToken = credentialInfo.get(GRAVITINO_ADLS_SAS_TOKEN);
    validate(accountName, sasToken, expireTimeInMS);
    this.accountName = accountName;
    this.sasToken = sasToken;
    this.expireTimeInMS = expireTimeInMS;
  }

  /**
   * Get ADLS account name
   *
   * @return The ADLS account name
   */
  public String accountName() {
    return accountName;
  }

  /**
   * Get ADLS SAS token.
   *
   * @return The ADLS SAS token.
   */
  public String sasToken() {
    return sasToken;
  }

  private void validate(String accountName, String sasToken, long expireTimeInMS) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accountName), "ADLS account name should not be empty.");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sasToken), "ADLS SAS token should not be empty.");
    Preconditions.checkArgument(
        expireTimeInMS > 0, "The expire time of ADLSTokenCredential should great than 0");
  }
}
