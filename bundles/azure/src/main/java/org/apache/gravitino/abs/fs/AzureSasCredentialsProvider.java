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

package org.apache.gravitino.abs.fs;

import java.io.IOException;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class AzureSasCredentialsProvider implements SASTokenProvider, Configurable {

  private Configuration configuration;
  private String sasToken;

  private GravitinoFileSystemCredentialsProvider gravitinoFileSystemCredentialsProvider;
  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.5D;

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public void initialize(Configuration conf, String accountName) throws IOException {
    this.configuration = conf;
    this.gravitinoFileSystemCredentialsProvider = FileSystemUtils.getGvfsCredentialProvider(conf);
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation) {
    // Refresh credentials if they are null or about to expire.
    if (sasToken == null || System.currentTimeMillis() >= expirationTime) {
      synchronized (this) {
        refresh();
      }
    }
    return sasToken;
  }

  private void refresh() {
    Credential[] gravitinoCredentials = gravitinoFileSystemCredentialsProvider.getCredentials();
    Credential credential = AzureStorageUtils.getADLSTokenCredential(gravitinoCredentials);
    if (credential == null) {
      throw new RuntimeException("No token credential for OSS found...");
    }

    if (credential instanceof ADLSTokenCredential) {
      ADLSTokenCredential adlsTokenCredential = (ADLSTokenCredential) credential;
      sasToken = adlsTokenCredential.sasToken();

      if (credential.expireTimeInMs() > 0) {
        expirationTime =
            System.currentTimeMillis()
                + (long)
                    ((credential.expireTimeInMs() - System.currentTimeMillis())
                        * EXPIRATION_TIME_FACTOR);
      }
    }
  }
}
