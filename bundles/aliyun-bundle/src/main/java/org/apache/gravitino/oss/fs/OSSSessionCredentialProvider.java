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

import static org.apache.gravitino.credential.OSSTokenCredential.GRAVITINO_OSS_SESSION_ACCESS_KEY_ID;
import static org.apache.gravitino.credential.OSSTokenCredential.GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY;
import static org.apache.gravitino.credential.OSSTokenCredential.GRAVITINO_OSS_TOKEN;

import com.aliyun.oss.common.auth.BasicCredentials;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import java.net.URI;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.hadoop.conf.Configuration;

public class OSSSessionCredentialProvider implements CredentialsProvider {

  private BasicCredentials basicCredentials;
  private String filesetIdentifier;
  private long expirationTime;
  private GravitinoClient client;

  public OSSSessionCredentialProvider(URI uri, Configuration conf) {

    // extra value and init Gravitino client here
    this.filesetIdentifier = conf.get("gravitino.fileset.identifier");
    String metalake = conf.get("fs.gravitino.client.metalake");
    String gravitinoServer = conf.get("fs.gravitino.server.uri");

    this.client =
        GravitinoClient.builder(gravitinoServer).withMetalake(metalake).withSimpleAuth().build();
  }

  @Override
  public void setCredentials(Credentials credentials) {}

  @Override
  public Credentials getCredentials() {
    // If the credentials are null or about to expire, refresh the credentials.
    if (basicCredentials == null || System.currentTimeMillis() > expirationTime - 5 * 60 * 1000) {
      synchronized (this) {
        refresh();
      }
    }

    return basicCredentials;
  }

  private void refresh() {
    // Refresh the credentials
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();

    @SuppressWarnings("unused")
    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));
    // Should mock
    // Credential credentials = fileset.supportsCredentials().getCredential("s3-token");

    Credential credentials =
        new S3TokenCredential("AS", "NF", "FwoGZXIvYXdzEDMaDBf3ltl7HG6K7Ne7QS", 1735033800000L);

    Map<String, String> credentialMap = credentials.credentialInfo();
    String accessKeyId = credentialMap.get(GRAVITINO_OSS_SESSION_ACCESS_KEY_ID);
    String secretAccessKey = credentialMap.get(GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY);
    String sessionToken = credentialMap.get(GRAVITINO_OSS_TOKEN);

    this.basicCredentials = new BasicCredentials(accessKeyId, secretAccessKey, sessionToken);
    this.expirationTime = credentials.expireTimeInMs();
  }
}
