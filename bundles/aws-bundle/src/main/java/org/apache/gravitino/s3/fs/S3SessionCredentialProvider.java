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

import static org.apache.gravitino.credential.S3TokenCredential.GRAVITINO_S3_SESSION_ACCESS_KEY_ID;
import static org.apache.gravitino.credential.S3TokenCredential.GRAVITINO_S3_SESSION_SECRET_ACCESS_KEY;
import static org.apache.gravitino.credential.S3TokenCredential.GRAVITINO_S3_TOKEN;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import java.net.URI;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.hadoop.conf.Configuration;

public class S3SessionCredentialProvider implements AWSCredentialsProvider {

  private final GravitinoClient client;
  private final String filesetIdentifier;

  private BasicSessionCredentials basicSessionCredentials;
  private long expirationTime;

  public S3SessionCredentialProvider(final URI uri, final Configuration conf) {
    // extra value and init Gravitino client here
    this.filesetIdentifier = conf.get("gravitino.fileset.identifier");
    String metalake = conf.get("fs.gravitino.client.metalake");
    String gravitinoServer = conf.get("fs.gravitino.server.uri");

    // TODO, support auth between client and server.
    this.client =
        GravitinoClient.builder(gravitinoServer).withMetalake(metalake).withSimpleAuth().build();
  }

  @Override
  public AWSCredentials getCredentials() {

    // Refresh credentials if they are null or about to expire in 5 minutes
    if (basicSessionCredentials == null
        || System.currentTimeMillis() > expirationTime - 5 * 60 * 1000) {
      synchronized (this) {
        refresh();
      }
    }

    return basicSessionCredentials;
  }

  @Override
  public void refresh() {
    // The format of filesetIdentifier is "metalake.catalog.fileset.schema"
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();

    @SuppressWarnings("unused")
    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));
    // Should mock
    // Credential credentials = fileset.supportsCredentials().getCredential("s3-token");

    Credential credentials = new S3TokenCredential("ASIAZ6", "NFzd", "xx", 1735033800000L);

    Map<String, String> credentialMap = credentials.credentialInfo();
    String accessKeyId = credentialMap.get(GRAVITINO_S3_SESSION_ACCESS_KEY_ID);
    String secretAccessKey = credentialMap.get(GRAVITINO_S3_SESSION_SECRET_ACCESS_KEY);
    String sessionToken = credentialMap.get(GRAVITINO_S3_TOKEN);

    this.basicSessionCredentials =
        new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken);
    this.expirationTime = credentials.expireTimeInMs();
  }
}
