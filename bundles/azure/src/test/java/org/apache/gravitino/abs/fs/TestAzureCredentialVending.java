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

package org.apache.gravitino.abs.fs;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.InMemoryFileSystemCredentialsProvider;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the credential configuration and in-memory vending paths for Azure credentials. */
public class TestAzureCredentialVending {

  private static final String ACCOUNT_NAME = "testaccount";
  private static final String ACCOUNT_HOST = ACCOUNT_NAME + ".dfs.core.windows.net";

  @Test
  void testAccountKeyCredential() {
    AzureAccountKeyCredential credential =
        new AzureAccountKeyCredential(ACCOUNT_NAME, "account-key");

    Map<String, String> credentialConf =
        new AzureFileSystemProvider().getFileSystemCredentialConf(new Credential[] {credential});

    Assertions.assertEquals(
        "account-key", credentialConf.get("fs.azure.account.key." + ACCOUNT_HOST));
  }

  @Test
  void testAdlsTokenCredential() throws IOException {
    ADLSTokenCredential credential =
        new ADLSTokenCredential(ACCOUNT_NAME, "sas-token", System.currentTimeMillis() + 60_000L);
    Credential[] credentials = new Credential[] {credential};
    String handle = InMemoryFileSystemCredentialsProvider.register(credentials);
    try {
      Configuration conf = new Configuration(false);
      Map<String, String> credentialConf =
          new AzureFileSystemProvider().getFileSystemCredentialConf(credentials);
      credentialConf.forEach(conf::set);
      conf.set(
          GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
          InMemoryFileSystemCredentialsProvider.class.getCanonicalName());
      conf.set(InMemoryFileSystemCredentialsProvider.CREDENTIAL_HANDLE, handle);

      Assertions.assertEquals(
          AuthType.SAS.name(),
          conf.get(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + "." + ACCOUNT_HOST));
      Assertions.assertEquals(
          AzureSasCredentialsProvider.class.getName(),
          conf.get(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE + "." + ACCOUNT_HOST));

      AzureSasCredentialsProvider provider = new AzureSasCredentialsProvider();
      provider.initialize(conf, ACCOUNT_HOST);
      Assertions.assertEquals(
          "sas-token", provider.getSASToken(ACCOUNT_HOST, "container", "path", "list"));
    } finally {
      InMemoryFileSystemCredentialsProvider.unregister(handle);
    }
  }
}
