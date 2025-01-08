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
import java.io.File;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.hadoop.conf.Configuration;

/** Utility class for Gravitino Virtual File System. */
public class GravitinoVirtualFileSystemUtils {

  /**
   * Get Gravitino client by the configuration.
   *
   * @param configuration The configuration for the Gravitino client.
   * @return The Gravitino client.
   */
  public static GravitinoClient createClient(Configuration configuration) {
    // initialize the Gravitino client
    String serverUri =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
    String metalakeValue =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(serverUri),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);

    String authType =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
            GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
    if (authType.equalsIgnoreCase(GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE)) {
      return GravitinoClient.builder(serverUri)
          .withMetalake(metalakeValue)
          .withSimpleAuth()
          .build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE)) {
      String authServerUri =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY,
          authServerUri);

      String credential =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY,
          credential);

      String path =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY,
          path);

      String scope =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY,
          scope);

      DefaultOAuth2TokenProvider authDataProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(authServerUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();

      return GravitinoClient.builder(serverUri)
          .withMetalake(metalakeValue)
          .withOAuth(authDataProvider)
          .build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE)) {
      String principal =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
          principal);
      String keytabFilePath =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration
                  .FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY);
      KerberosTokenProvider authDataProvider;
      if (StringUtils.isNotBlank(keytabFilePath)) {
        // Using principal and keytab to create auth provider
        authDataProvider =
            KerberosTokenProvider.builder()
                .withClientPrincipal(principal)
                .withKeyTabFile(new File(keytabFilePath))
                .build();
      } else {
        // Using ticket cache to create auth provider
        authDataProvider = KerberosTokenProvider.builder().withClientPrincipal(principal).build();
      }

      return GravitinoClient.builder(serverUri)
          .withMetalake(metalakeValue)
          .withKerberosAuth(authDataProvider)
          .build();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported authentication type: %s for %s.",
              authType, GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));
    }
  }

  private static void checkAuthConfig(String authType, String configKey, String configValue) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configValue),
        "%s should not be null if %s is set to %s.",
        configKey,
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        authType);
  }
}
