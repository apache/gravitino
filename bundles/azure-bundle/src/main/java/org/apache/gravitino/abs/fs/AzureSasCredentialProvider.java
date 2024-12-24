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

import static org.apache.gravitino.credential.ADLSTokenCredential.GRAVITINO_ADLS_SAS_TOKEN;

import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.security.AccessControlException;

public class AzureSasCredentialProvider implements SASTokenProvider, Configurable {

  private Configuration configuration;

  @SuppressWarnings("unused")
  private String filesetIdentifier;

  @SuppressWarnings("unused")
  private GravitinoClient client;

  private String sasToken;
  private long expirationTime;

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
    this.filesetIdentifier = conf.get("gravitino.fileset.identifier");

    @SuppressWarnings("unused")
    String metalake = conf.get("fs.gravitino.client.metalake");
    @SuppressWarnings("unused")
    String gravitinoServer = conf.get("fs.gravitino.server.uri");

    // TODO, support auth between client and server.
    this.client =
        GravitinoClient.builder(gravitinoServer).withMetalake(metalake).withSimpleAuth().build();
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation)
      throws IOException, AccessControlException {
    // Refresh credentials if they are null or about to expire in 5 minutes
    if (sasToken == null || System.currentTimeMillis() > expirationTime - 5 * 60 * 1000) {
      synchronized (this) {
        refresh();
      }
    }
    return sasToken;
  }

  private void refresh() {
    // The format of filesetIdentifier is "metalake.catalog.fileset.schema"
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();

    @SuppressWarnings("unused")
    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));
    // Should mock
    // Credential credentials = fileset.supportsCredentials().getCredential("s3-token");

    Credential credential = new ADLSTokenCredential("xxx", "xxx", 1L);

    Map<String, String> credentialMap = credential.credentialInfo();
    this.sasToken = credentialMap.get(GRAVITINO_ADLS_SAS_TOKEN);
    this.expirationTime = credential.expireTimeInMs();
  }
}
