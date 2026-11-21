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
package org.apache.gravitino.s3.fs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import java.net.URI;
import org.apache.gravitino.s3.fs.S3CredentialsRefresher.S3Credentials;
import org.apache.hadoop.conf.Configuration;

/**
 * An AWS SDK v1 credentials provider that vends the S3 credentials of Gravitino. It is used by
 * Hadoop 3.3 and earlier, whose S3A connector is built on the AWS SDK v1. Hadoop 3.4 and later use
 * {@link S3CredentialsProviderV2}.
 */
public class S3CredentialsProvider implements AWSCredentialsProvider {

  private final S3CredentialsRefresher refresher;

  /**
   * Creates the provider. Hadoop instantiates it by reflection with this signature.
   *
   * @param uri The URI of the file system.
   * @param conf The Hadoop configuration that carries the Gravitino credential provider settings.
   */
  public S3CredentialsProvider(final URI uri, final Configuration conf) {
    this.refresher = new S3CredentialsRefresher(conf);
  }

  @Override
  public AWSCredentials getCredentials() {
    S3Credentials credentials = refresher.getCredentials();
    if (credentials.sessionToken() == null) {
      return new BasicAWSCredentials(credentials.accessKeyId(), credentials.secretAccessKey());
    }
    return new BasicSessionCredentials(
        credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());
  }

  @Override
  public void refresh() {
    refresher.refresh();
  }
}
