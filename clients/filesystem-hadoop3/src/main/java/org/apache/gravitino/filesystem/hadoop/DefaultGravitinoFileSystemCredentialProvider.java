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

package org.apache.gravitino.filesystem.hadoop;

import com.google.common.base.Preconditions;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.hadoop.conf.Configuration;

public class DefaultGravitinoFileSystemCredentialProvider
    implements GravitinoFileSystemCredentialProvider {

  private Configuration configuration;

  private static final Pattern IDENTIFIER_PATTERN =
      Pattern.compile("^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?>/[^/]+)*/?$");

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public Credential[] getCredentials() {
    String virtualPath = configuration.get(GVFS_CREDENTIAL_PROVIDER_PATH);
    NameIdentifier nameIdentifier = getNameIdentifierFromVirtualPath(virtualPath);
    String[] idents = nameIdentifier.namespace().levels();
    try (GravitinoClient client = GravitinoVirtualFileSystemUtils.createClient(configuration)) {
      FilesetCatalog filesetCatalog = client.loadCatalog(idents[0]).asFilesetCatalog();
      Fileset fileset =
          filesetCatalog.loadFileset(NameIdentifier.of(idents[1], nameIdentifier.name()));
      return fileset.supportsCredentials().getCredentials();
    }
  }

  private NameIdentifier getNameIdentifierFromVirtualPath(String gravitinoVirtualPath) {
    String virtualPath = gravitinoVirtualPath.toString();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(virtualPath),
        "Uri which need be extracted cannot be null or empty.");

    Matcher matcher = IDENTIFIER_PATTERN.matcher(virtualPath);
    Preconditions.checkArgument(
        matcher.matches() && matcher.groupCount() == 3,
        "URI %s doesn't contains valid identifier",
        virtualPath);

    // The format is `catalog.schema.fileset`
    return NameIdentifier.of(matcher.group(1), matcher.group(2), matcher.group(3));
  }
}
