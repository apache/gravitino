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

package org.apache.gravitino.gcs.fs;

import com.google.cloud.hadoop.util.AccessTokenProvider.AccessToken;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.InMemoryFileSystemCredentialsProvider;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the complete in-memory credential vending path for GCS access tokens. */
public class TestGCSCredentialVending {

  @Test
  void testTokenCredential() {
    long expirationTime = System.currentTimeMillis() + 60_000L;
    GCSTokenCredential credential = new GCSTokenCredential("access-token", expirationTime);
    Credential[] credentials = new Credential[] {credential};
    String handle = InMemoryFileSystemCredentialsProvider.register(credentials);
    try {
      Configuration conf = new Configuration(false);
      Map<String, String> credentialConf =
          new GCSFileSystemProvider().getFileSystemCredentialConf(credentials);
      credentialConf.forEach(conf::set);
      conf.set(
          GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
          InMemoryFileSystemCredentialsProvider.class.getCanonicalName());
      conf.set(InMemoryFileSystemCredentialsProvider.CREDENTIAL_HANDLE, handle);
      Assertions.assertEquals(
          GCSCredentialsProvider.class.getName(),
          conf.get("fs.gs.auth.access.token.provider.impl"));

      GCSCredentialsProvider provider = new GCSCredentialsProvider();
      provider.setConf(conf);
      AccessToken actual = provider.getAccessToken();

      Assertions.assertEquals("access-token", actual.getToken());
      Assertions.assertEquals(expirationTime, actual.getExpirationTimeMilliSeconds());
    } finally {
      InMemoryFileSystemCredentialsProvider.unregister(handle);
    }
  }
}
