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

/** ADLS account key credential. */
public class ADLSAccountKeyCredential implements Credential {

  /** ADLS account key credential type. */
  public static final String ADLS_ACCOUNT_KEY_CREDENTIAL_TYPE = "adls-account-key";
  /** ADLS storage account name */
  public static final String GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME = "azure-storage-account-name";
  /** ADLS storage account key */
  public static final String GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY = "azure-storage-account-key";

  private String accountName;
  private String accountKey;

  /**
   * Constructs an instance of {@link ADLSAccountKeyCredential}.
   *
   * @param accountName The ADLS account name.
   * @param accountKey The ADLS account key.
   */
  public ADLSAccountKeyCredential(String accountName, String accountKey) {
    validate(accountName, accountKey, 0);
    this.accountName = accountName;
    this.accountKey = accountKey;
  }

  /**
   * This is the constructor that is used by credential factory to create an instance of credential
   * according to the credential information.
   */
  public ADLSAccountKeyCredential() {}

  @Override
  public String credentialType() {
    return ADLS_ACCOUNT_KEY_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return 0;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME, accountName)
        .put(GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY, accountKey)
        .build();
  }

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMS) {
    String accountName = credentialInfo.get(GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
    String accountKey = credentialInfo.get(GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    validate(accountName, accountKey, expireTimeInMS);
    this.accountName = accountName;
    this.accountKey = accountKey;
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
   * Get ADLS account key
   *
   * @return The ADLS account key
   */
  public String accountKey() {
    return accountKey;
  }

  private void validate(String accountName, String accountKey, long expireTimeInMS) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accountName), "ADLS account name should not be empty.");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accountKey), "ADLS account key should not be empty.");
    Preconditions.checkArgument(
        expireTimeInMS == 0, "The expire time of ADLSAccountKeyCredential is not 0");
  }
}
