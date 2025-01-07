/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.oss.fs;

import com.aliyun.oss.common.auth.BasicCredentials;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import java.net.URI;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialProvider;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.hadoop.conf.Configuration;

public class TestOSSCredentialProvider implements CredentialsProvider {
  private GravitinoFileSystemCredentialProvider gravitinoFileSystemCredentialProvider;
  private Credentials basicCredentials;
  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.9D;

  public TestOSSCredentialProvider(URI uri, Configuration conf) {
    try {
      gravitinoFileSystemCredentialProvider =
          (GravitinoFileSystemCredentialProvider)
              Class.forName(conf.get("fs.gvfs.credential.provider"))
                  .getDeclaredConstructor()
                  .newInstance();
      gravitinoFileSystemCredentialProvider.setConf(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create GravitinoFileSystemCredentialProvider", e);
    }
  }

  @Override
  public void setCredentials(Credentials credentials) {}

  @Override
  public Credentials getCredentials() {
    if (basicCredentials == null || System.currentTimeMillis() >= expirationTime) {
      Credential[] gravitinoCredentials = gravitinoFileSystemCredentialProvider.getCredentials();
      if (gravitinoCredentials.length == 0) {
        throw new RuntimeException("No credentials found");
      }

      // Get dynamic credentials from Gravitino
      Credential gravitinoCredential = gravitinoCredentials[0];

      if (gravitinoCredential instanceof OSSSecretKeyCredential) {
        OSSSecretKeyCredential ossSecretKeyCredential =
            (OSSSecretKeyCredential) gravitinoCredential;
        basicCredentials =
            new DefaultCredentials(
                ossSecretKeyCredential.accessKeyId(), ossSecretKeyCredential.secretAccessKey());
      } else if (gravitinoCredential instanceof OSSTokenCredential) {
        OSSTokenCredential ossTokenCredential = (OSSTokenCredential) gravitinoCredential;
        basicCredentials =
            new BasicCredentials(
                ossTokenCredential.accessKeyId(),
                ossTokenCredential.secretAccessKey(),
                ossTokenCredential.securityToken());
      }

      if (gravitinoCredential.expireTimeInMs() > 0) {
        expirationTime =
            System.currentTimeMillis()
                + (long)
                    ((gravitinoCredential.expireTimeInMs() - System.currentTimeMillis())
                        * EXPIRATION_TIME_FACTOR);
      }
    }

    return basicCredentials;
  }
}
