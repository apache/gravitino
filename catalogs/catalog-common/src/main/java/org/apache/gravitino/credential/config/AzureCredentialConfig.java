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

package org.apache.gravitino.credential.config;

import java.util.Map;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.storage.AzureProperties;

public class AzureCredentialConfig extends Config {

  public static final ConfigEntry<String> AZURE_STORAGE_ACCOUNT_NAME =
      new ConfigBuilder(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME)
          .doc("The name of the Azure Data Lake Storage account.")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> AZURE_STORAGE_ACCOUNT_KEY =
      new ConfigBuilder(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY)
          .doc("The key of the Azure Data Lake Storage account.")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> AZURE_TENANT_ID =
      new ConfigBuilder(AzureProperties.GRAVITINO_AZURE_TENANT_ID)
          .doc("The Azure Active Directory (AAD) tenant ID used for authentication.")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> AZURE_CLIENT_ID =
      new ConfigBuilder(AzureProperties.GRAVITINO_AZURE_CLIENT_ID)
          .doc("The client ID used for authenticating with Azure Active Directory (AAD).")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> AZURE_CLIENT_SECRET =
      new ConfigBuilder(AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET)
          .doc("The client secret used for authenticating with Azure Active Directory (AAD).")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<Integer> ADLS_TOKEN_EXPIRE_IN_SECS =
      new ConfigBuilder(CredentialConstants.ADLS_TOKEN_EXPIRE_IN_SECS)
          .doc(
              "The expiration time (in seconds) for the Azure Active Directory (AAD) authentication token.")
          .version(ConfigConstants.VERSION_0_7_0)
          .intConf()
          .createWithDefault(3600);

  public AzureCredentialConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  @NotNull
  public String storageAccountName() {
    return this.get(AZURE_STORAGE_ACCOUNT_NAME);
  }

  @NotNull
  public String storageAccountKey() {
    return this.get(AZURE_STORAGE_ACCOUNT_KEY);
  }

  @NotNull
  public String tenantId() {
    return this.get(AZURE_TENANT_ID);
  }

  @NotNull
  public String clientId() {
    return this.get(AZURE_CLIENT_ID);
  }

  @NotNull
  public String clientSecret() {
    return this.get(AZURE_CLIENT_SECRET);
  }

  @NotNull
  public Integer adlsTokenExpireInSecs() {
    return this.get(ADLS_TOKEN_EXPIRE_IN_SECS);
  }
}
