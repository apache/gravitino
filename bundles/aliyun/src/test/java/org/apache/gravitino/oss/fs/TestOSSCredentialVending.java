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

package org.apache.gravitino.oss.fs;

import com.aliyun.oss.common.auth.Credentials;
import java.net.URI;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.InMemoryFileSystemCredentialsProvider;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the complete in-memory credential vending path for OSS credentials. */
public class TestOSSCredentialVending {

  private static final URI OSS_URI = URI.create("oss://test-bucket/");

  @Test
  void testSecretKeyCredential() {
    OSSSecretKeyCredential credential = new OSSSecretKeyCredential("access-key", "secret-key");

    Credentials actual = readCredential(credential);

    Assertions.assertEquals("access-key", actual.getAccessKeyId());
    Assertions.assertEquals("secret-key", actual.getSecretAccessKey());
    Assertions.assertFalse(actual.useSecurityToken());
  }

  @Test
  void testTokenCredential() {
    OSSTokenCredential credential =
        new OSSTokenCredential(
            "access-key", "secret-key", "security-token", System.currentTimeMillis() + 60_000L);

    Credentials actual = readCredential(credential);

    Assertions.assertEquals("access-key", actual.getAccessKeyId());
    Assertions.assertEquals("secret-key", actual.getSecretAccessKey());
    Assertions.assertEquals("security-token", actual.getSecurityToken());
    Assertions.assertTrue(actual.useSecurityToken());
  }

  private Credentials readCredential(Credential credential) {
    Credential[] credentials = new Credential[] {credential};
    String handle = InMemoryFileSystemCredentialsProvider.register(credentials);
    try {
      Configuration conf = new Configuration(false);
      Map<String, String> credentialConf =
          new OSSFileSystemProvider().getFileSystemCredentialConf(credentials);
      credentialConf.forEach(conf::set);
      conf.set(
          GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
          InMemoryFileSystemCredentialsProvider.class.getCanonicalName());
      conf.set(InMemoryFileSystemCredentialsProvider.CREDENTIAL_HANDLE, handle);
      Assertions.assertEquals(
          OSSCredentialsProvider.class.getCanonicalName(),
          conf.get(Constants.CREDENTIALS_PROVIDER_KEY));

      return new OSSCredentialsProvider(OSS_URI, conf).getCredentials();
    } finally {
      InMemoryFileSystemCredentialsProvider.unregister(handle);
    }
  }
}
