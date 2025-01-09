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
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.hadoop.conf.Configuration;

public class OSSCredentialsProvider implements CredentialsProvider {

  private GravitinoFileSystemCredentialsProvider gravitinoFileSystemCredentialsProvider;
  private Credentials basicCredentials;
  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.5D;

  public OSSCredentialsProvider(URI uri, Configuration conf) {
    this.gravitinoFileSystemCredentialsProvider = FileSystemUtils.getGvfsCredentialProvider(conf);
  }

  @Override
  public void setCredentials(Credentials credentials) {}

  @Override
  public Credentials getCredentials() {
    if (basicCredentials == null || System.currentTimeMillis() >= expirationTime) {
      synchronized (this) {
        refresh();
      }
    }

    return basicCredentials;
  }

  private void refresh() {
    Credential[] gravitinoCredentials = gravitinoFileSystemCredentialsProvider.getCredentials();
    Credential credential = OSSUtils.getSuitableCredential(gravitinoCredentials);
    if (credential == null) {
      throw new RuntimeException("No suitable credential for OSS found...");
    }

    if (credential instanceof OSSSecretKeyCredential) {
      OSSSecretKeyCredential ossSecretKeyCredential = (OSSSecretKeyCredential) credential;
      basicCredentials =
          new DefaultCredentials(
              ossSecretKeyCredential.accessKeyId(), ossSecretKeyCredential.secretAccessKey());
    } else if (credential instanceof OSSTokenCredential) {
      OSSTokenCredential ossTokenCredential = (OSSTokenCredential) credential;
      basicCredentials =
          new BasicCredentials(
              ossTokenCredential.accessKeyId(),
              ossTokenCredential.secretAccessKey(),
              ossTokenCredential.securityToken());
    }

    if (credential.expireTimeInMs() > 0) {
      expirationTime =
          System.currentTimeMillis()
              + (long)
                  ((credential.expireTimeInMs() - System.currentTimeMillis())
                      * EXPIRATION_TIME_FACTOR);
    }
  }
}
