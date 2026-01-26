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

import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CURRENT_LOCATION_NAME;

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.hadoop.conf.Configuration;

/**
 * Default implementation of {@link GravitinoFileSystemCredentialsProvider} which provides
 * credentials for Gravitino Virtual File System.
 */
public class DefaultGravitinoFileSystemCredentialsProvider
    implements GravitinoFileSystemCredentialsProvider {

  private Configuration configuration;

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
    // The format of name identifier is `metalake.catalog.schema.fileset`
    String nameIdentifier = configuration.get(GVFS_NAME_IDENTIFIER);
    String[] idents = nameIdentifier.split("\\.");

    try (GravitinoClient client = GravitinoVirtualFileSystemUtils.createClient(configuration)) {
      FilesetCatalog filesetCatalog = client.loadCatalog(idents[1]).asFilesetCatalog();
      Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));

      String targetLocationName = getTargetLocation(fileset);
      GravitinoVirtualFileSystemUtils.setCallerContextForGetCredentials(targetLocationName);

      return fileset.supportsCredentials().getCredentials();
    } finally {
      CallerContext.CallerContextHolder.remove();
    }
  }

  private String getTargetLocation(Fileset fileset) {
    String currentLocationEnvVar =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CURRENT_LOCATION_NAME_ENV_VAR,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_CURRENT_LOCATION_NAME_ENV_VAR_DEFAULT);
    String locationName =
        Optional.ofNullable(configuration.get(FS_GRAVITINO_CURRENT_LOCATION_NAME))
            .orElse(System.getenv(currentLocationEnvVar));
    return locationName == null
        ? fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME)
        : locationName;
  }
}
