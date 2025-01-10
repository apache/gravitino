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

/** Azure account key credential. */
public class AzureAccountKeyCredential implements Credential {

  /** Azure account key credential type. */
  public static final String AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE = "azure-account-key";
  /** Azure storage account name */
  public static final String GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME = "azure-storage-account-name";
  /** Azure storage account key */
  public static final String GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY = "azure-storage-account-key";

  private String accountName;
  private String accountKey;

  /**
   * Constructs an instance of {@link AzureAccountKeyCredential}.
   *
   * @param accountName The Azure account name.
   * @param accountKey The Azure account key.
   */
  public AzureAccountKeyCredential(String accountName, String accountKey) {
    validate(accountName, accountKey);
    this.accountName = accountName;
    this.accountKey = accountKey;
  }

  /**
   * This is the constructor that is used by credential factory to create an instance of credential
   * according to the credential information.
   */
  public AzureAccountKeyCredential() {}

  @Override
  public String credentialType() {
    return AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE;
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
    validate(accountName, accountKey);
    this.accountName = accountName;
    this.accountKey = accountKey;
  }

  /**
   * Get Azure account name
   *
   * @return The Azure account name
   */
  public String accountName() {
    return accountName;
  }

  /**
   * Get Azure account key
   *
   * @return The Azure account key
   */
  public String accountKey() {
    return accountKey;
  }

  private void validate(String accountName, String accountKey) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accountName), "Azure account name should not be empty.");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accountKey), "Azure account key should not be empty.");
  }
}
