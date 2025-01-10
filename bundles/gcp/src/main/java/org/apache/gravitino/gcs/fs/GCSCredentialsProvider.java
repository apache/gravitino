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

import com.google.cloud.hadoop.util.AccessTokenProvider;
import java.io.IOException;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.hadoop.conf.Configuration;

public class GCSCredentialsProvider implements AccessTokenProvider {
  private Configuration configuration;
  private GravitinoFileSystemCredentialsProvider gravitinoFileSystemCredentialsProvider;

  private AccessToken accessToken;
  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.5D;

  @Override
  public AccessToken getAccessToken() {
    if (accessToken == null || System.currentTimeMillis() >= expirationTime) {
      try {
        refresh();
      } catch (IOException e) {
        throw new RuntimeException("Failed to refresh access token", e);
      }
    }
    return accessToken;
  }

  @Override
  public void refresh() throws IOException {
    Credential[] gravitinoCredentials = gravitinoFileSystemCredentialsProvider.getCredentials();

    Credential credential = GCSUtils.getGCSTokenCredential(gravitinoCredentials);
    if (credential == null) {
      throw new RuntimeException("No suitable credential for OSS found...");
    }

    if (credential instanceof GCSTokenCredential) {
      GCSTokenCredential gcsTokenCredential = (GCSTokenCredential) credential;
      accessToken = new AccessToken(gcsTokenCredential.token(), credential.expireTimeInMs());

      if (credential.expireTimeInMs() > 0) {
        expirationTime =
            System.currentTimeMillis()
                + (long)
                    ((credential.expireTimeInMs() - System.currentTimeMillis())
                        * EXPIRATION_TIME_FACTOR);
      }
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
    this.gravitinoFileSystemCredentialsProvider =
        FileSystemUtils.getGvfsCredentialProvider(configuration);
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }
}
